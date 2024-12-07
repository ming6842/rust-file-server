use actix_web::{
    http::header, web, web::Bytes, App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use chrono::Utc;
use futures::StreamExt;
use log::{debug, error, info, warn};
use serde::Serialize;
use serde_xml_rs::to_string;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time;

#[derive(Serialize)]
struct ListBucketResult {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "KeyCount")]
    key_count: i32,
    #[serde(rename = "MaxKeys")]
    max_keys: i32,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "Contents")]
    contents: Vec<Contents>,
}

#[derive(Serialize)]
struct Contents {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "LastModified")]
    last_modified: String,
    #[serde(rename = "ETag")]
    etag: String,
    #[serde(rename = "Size")]
    size: i64,
    #[serde(rename = "StorageClass")]
    storage_class: String,
}

#[derive(Debug)]
struct ByteRange {
    start: u64,
    end: Option<u64>,
}

impl ByteRange {
    fn parse(range_header: &str, file_size: u64) -> Option<ByteRange> {
        let range_str = range_header.strip_prefix("bytes=")?;
        let mut parts = range_str.split('-');

        let start_str = parts.next()?;
        let end_str = parts.next()?;

        let start = if start_str.is_empty() {
            if let Ok(last_n) = end_str.parse::<u64>() {
                if last_n > file_size {
                    0
                } else {
                    file_size - last_n
                }
            } else {
                return None;
            }
        } else {
            start_str.parse::<u64>().ok()?
        };

        let end = if end_str.is_empty() {
            Some(file_size - 1)
        } else {
            let parsed_end = end_str.parse::<u64>().ok()?;
            Some(std::cmp::min(parsed_end, file_size - 1))
        };

        if start > file_size - 1 || end.map_or(false, |e| e < start) {
            None
        } else {
            Some(ByteRange { start, end })
        }
    }
}

struct DownloadProgress {
    client_info: String,
    filename: String,
    total_size: u64,
    bytes_sent: u64,
    start_time: Instant,
    last_chunk_time: Instant, // Added to track time between chunks
}

struct AppState {
    storage_path: PathBuf,
    downloads: Arc<Mutex<HashMap<String, DownloadProgress>>>,
}

fn get_client_info(req: &HttpRequest) -> String {
    let ip = req
        .peer_addr()
        .map_or("unknown".to_string(), |addr| addr.to_string());
    let user_agent = req
        .headers()
        .get("user-agent")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("unknown");
    format!("Client IP: {}, User-Agent: {}", ip, user_agent)
}

async fn progress_logger(downloads: Arc<Mutex<HashMap<String, DownloadProgress>>>) {
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let mut guard = downloads.lock().await;

        for (_, progress) in guard.iter() {
            let elapsed = progress.start_time.elapsed();
            let speed = if elapsed.as_secs() > 0 {
                progress.bytes_sent / elapsed.as_secs()
            } else {
                0
            };

            info!(
                "\r{} - {} - {}/{} bytes ({:.2}%) - {:.2} MB/s",
                progress.client_info,
                progress.filename,
                progress.bytes_sent,
                progress.total_size,
                (progress.bytes_sent as f64 / progress.total_size as f64) * 100.0,
                speed as f64 / (1024.0 * 1024.0)
            );
        }

        guard.retain(|_, progress| progress.bytes_sent < progress.total_size);
    }
}

async fn list_objects(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    let start = Instant::now();
    info!(
        "List objects request received from {}",
        get_client_info(&req)
    );

    let now = Utc::now().format("%Y-%m-%dT%H:%M:%S.000Z").to_string();
    let mut contents = Vec::new();
    let mut total_size: u64 = 0;
    let mut file_count = 0;

    if let Ok(entries) = fs::read_dir(&data.storage_path) {
        debug!("Reading directory: {:?}", data.storage_path);

        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    if let Ok(file_name) = entry.file_name().into_string() {
                        total_size += metadata.len();
                        file_count += 1;

                        debug!("Found file: {}, size: {} bytes", file_name, metadata.len());

                        contents.push(Contents {
                            key: file_name,
                            last_modified: now.clone(),
                            etag: format!("\"{}\"", metadata.len()),
                            size: metadata.len() as i64,
                            storage_class: "STANDARD".to_string(),
                        });
                    }
                }
            }
        }
    }

    let result = ListBucketResult {
        name: "mock-bucket".to_string(),
        prefix: "".to_string(),
        key_count: contents.len() as i32,
        max_keys: 1000,
        is_truncated: false,
        contents,
    };

    let xml = to_string(&result).unwrap_or_default();

    info!(
        "List operation completed in {:?}. Found {} files, total size: {} bytes",
        start.elapsed(),
        file_count,
        total_size
    );

    HttpResponse::Ok().content_type("application/xml").body(xml)
}
async fn get_object(
    req: HttpRequest,
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> impl Responder {
    let client_info = get_client_info(&req);
    info!(
        "Get object request received for '{}' from {}",
        path, client_info
    );

    let file_path = data.storage_path.join(&*path);
    debug!("Attempting to read file: {:?}", file_path);

    let metadata = match fs::metadata(&file_path) {
        Ok(m) => m,
        Err(e) => {
            warn!("File '{}' not found or not accessible: {}", path, e);
            return HttpResponse::NotFound().finish();
        }
    };

    let file_size = metadata.len();

    // Calculate MD5 hash for ETag
    let mut file = match fs::File::open(&file_path) {
        Ok(file) => file,
        Err(e) => {
            error!("Failed to open file: {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    // Read the file content for MD5 calculation
    let mut buffer = Vec::new();
    if let Err(e) = std::io::Read::read_to_end(&mut file, &mut buffer) {
        error!("Failed to read file for MD5 calculation: {}", e);
        return HttpResponse::InternalServerError().finish();
    }

    // Calculate MD5 hash and other response headers
    let etag = format!("\"{:x}\"", md5::compute(&buffer));
    let last_modified = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();

    const RATE_LIMIT: u64 = 1000; // bytes per second
    const CHUNK_SIZE: usize = 1000; // bytes per chunk

    if let Some(range_header) = req.headers().get(header::RANGE) {
        if let Some(range_str) = range_header.to_str().ok() {
            debug!("Range header received: {}", range_str);
    
            if let Some(byte_range) = ByteRange::parse(range_str, file_size) {
                debug!(
                    "Parsed range: start={}, end={:?}",
                    byte_range.start, byte_range.end
                );
    
                if let Ok(file) = fs::File::open(&file_path) {
                    let end = byte_range.end.unwrap_or(file_size - 1);
                    let length = end - byte_range.start + 1;
    
                    let download_id = format!("{}-{}-partial", Utc::now().timestamp_millis(), path);
                    let progress = DownloadProgress {
                        client_info: client_info.clone(),
                        filename: path.to_string(),
                        total_size: length,
                        bytes_sent: 0,
                        start_time: Instant::now(),
                        last_chunk_time: Instant::now(),
                    };
    
                    data.downloads
                        .lock()
                        .await
                        .insert(download_id.clone(), progress);
    
                    let file = tokio::fs::File::from_std(file);
                    
                    // Create a custom stream that respects the byte range
                    let stream = tokio_util::io::ReaderStream::new(file)
                        .skip(byte_range.start as usize / CHUNK_SIZE)
                        .take((length as usize + CHUNK_SIZE - 1) / CHUNK_SIZE);
    
                    let downloads = Arc::clone(&data.downloads);
                    let download_id_clone = download_id.clone();
    
                    let rate_limited_stream = stream.then(move |result| {
                        let downloads = downloads.clone();
                        let download_id = download_id_clone.clone();
    
                        async move {
                            match result {
                                Ok(chunk) => {
                                    let mut remaining_chunk = chunk;
                                    let mut processed_chunk = Vec::new();
    
                                    while !remaining_chunk.is_empty() {
                                        let current_size = std::cmp::min(remaining_chunk.len(), CHUNK_SIZE);
                                        let current_chunk = remaining_chunk.slice(0..current_size);
                                        remaining_chunk = remaining_chunk.slice(current_size..);
    
                                        let chunk_len = current_chunk.len() as u64;
    
                                        // Update progress and apply rate limiting
                                        let mut guard = downloads.lock().await;
                                        if let Some(progress) = guard.get_mut(&download_id) {
                                            let elapsed = progress.last_chunk_time.elapsed();
                                            let required_delay = Duration::from_secs_f64(
                                                chunk_len as f64 / RATE_LIMIT as f64,
                                            );
    
                                            // If we need to wait longer to maintain the rate limit, sleep
                                            if elapsed < required_delay {
                                                let sleep_duration = required_delay - elapsed;
                                                drop(guard); // Release the lock before sleeping
                                                time::sleep(sleep_duration).await;
                                                guard = downloads.lock().await;
                                                if let Some(progress) = guard.get_mut(&download_id) {
                                                    progress.bytes_sent += chunk_len;
                                                    progress.last_chunk_time = Instant::now();
                                                }
                                            } else {
                                                progress.bytes_sent += chunk_len;
                                                progress.last_chunk_time = Instant::now();
                                            }
                                        }
    
                                        processed_chunk.extend_from_slice(&current_chunk);
                                    }
    
                                    Ok(Bytes::from(processed_chunk))
                                }
                                Err(e) => Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    e.to_string(),
                                )),
                            }
                        }
                    });
                    info!(
                        "Serving partial content for '{}': bytes {}-{}/{} to {}",
                        path, byte_range.start, end, file_size, client_info
                    );
    
                    let mut response = HttpResponse::PartialContent();
                    return response
                        .append_header((
                            "Content-Range",
                            format!("bytes {}-{}/{}", byte_range.start, end, file_size),
                        ))
                        .append_header(("Content-Length", length.to_string()))
                        .append_header(("Last-Modified", last_modified))
                        .append_header(("ETag", etag))
                        .append_header(("Accept-Ranges", "bytes"))
                        .append_header(("x-amz-server-side-encryption", "AES256"))
                        .append_header(("Content-Type", "binary/octet-stream"))
                        .append_header(("Server", "AmazonS3"))
                        .no_chunking(length) // Pass the length parameter
                        .streaming(rate_limited_stream);
                }
            }
        }
        return HttpResponse::RangeNotSatisfiable()
            .append_header(("Content-Range", format!("bytes */{}", file_size)))
            .finish();
    }
    // Create a new file handle for streaming
    let std_file = match fs::File::open(&file_path) {
        Ok(file) => file,
        Err(e) => {
            error!("Failed to open file for streaming: {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let download_id = format!("{}-{}", Utc::now().timestamp_millis(), path);
    let progress = DownloadProgress {
        client_info: client_info.clone(),
        filename: path.to_string(),
        total_size: file_size,
        bytes_sent: 0,
        start_time: Instant::now(),
        last_chunk_time: Instant::now(),
    };

    data.downloads
        .lock()
        .await
        .insert(download_id.clone(), progress);

    let file = tokio::fs::File::from_std(std_file);
    let stream = tokio_util::io::ReaderStream::new(file);

    let downloads = Arc::clone(&data.downloads);
    let download_id_clone = download_id.clone();

    let rate_limited_stream = stream.then(move |result| {
        let downloads = downloads.clone();
        let download_id = download_id_clone.clone();

        async move {
            match result {
                Ok(chunk) => {
                    let mut remaining_chunk = chunk;
                    let mut processed_chunk = Vec::new();

                    while !remaining_chunk.is_empty() {
                        let current_size = std::cmp::min(remaining_chunk.len(), CHUNK_SIZE);
                        let current_chunk = remaining_chunk.slice(0..current_size);
                        remaining_chunk = remaining_chunk.slice(current_size..);

                        let chunk_len = current_chunk.len() as u64;

                        // Update progress and apply rate limiting
                        let mut guard = downloads.lock().await;
                        if let Some(progress) = guard.get_mut(&download_id) {
                            let elapsed = progress.last_chunk_time.elapsed();
                            let required_delay =
                                Duration::from_secs_f64(chunk_len as f64 / RATE_LIMIT as f64);

                            // If we need to wait longer to maintain the rate limit, sleep
                            if elapsed < required_delay {
                                let sleep_duration = required_delay - elapsed;
                                drop(guard); // Release the lock before sleeping
                                time::sleep(sleep_duration).await;
                                guard = downloads.lock().await;
                                if let Some(progress) = guard.get_mut(&download_id) {
                                    progress.bytes_sent += chunk_len;
                                    progress.last_chunk_time = Instant::now();
                                }
                            } else {
                                progress.bytes_sent += chunk_len;
                                progress.last_chunk_time = Instant::now();
                            }
                        }

                        processed_chunk.extend_from_slice(&current_chunk);
                    }

                    Ok(Bytes::from(processed_chunk))
                }
                Err(e) => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )),
            }
        }
    });

    let mut response = HttpResponse::Ok();
    response
        .append_header(("Last-Modified", last_modified))
        .append_header(("ETag", etag))
        .append_header(("x-amz-server-side-encryption", "AES256"))
        .append_header(("Accept-Ranges", "bytes"))
        .append_header(("Content-Type", "binary/octet-stream"))
        .append_header(("Content-Length", file_size.to_string()))
        .append_header(("Server", "AmazonS3"))
        .append_header(("Connection", "close"));

    response.streaming(rate_limited_stream)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            use std::io::Write;
            let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f");
            writeln!(
                buf,
                "[{}] {} - {}",
                timestamp,
                record.level(),
                record.args()
            )
        })
        .init();

    let storage_path = PathBuf::from("storage");
    if let Err(e) = fs::create_dir_all(&storage_path) {
        error!("Failed to create storage directory: {}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to create storage directory",
        ));
    }

    let downloads = Arc::new(Mutex::new(HashMap::new()));
    let downloads_clone = Arc::clone(&downloads);

    tokio::spawn(async move {
        progress_logger(downloads_clone).await;
    });

    let host = std::env::var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("SERVER_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .unwrap_or(8080);

    info!("⚠️  Server starting in PUBLIC mode");
    info!("🌐 Binding to {}:{}", host, port);
    info!("📂 Serving files from ./storage directory");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                storage_path: storage_path.clone(),
                downloads: Arc::clone(&downloads),
            }))
            .wrap(actix_web::middleware::Logger::default())
            .route("/", web::get().to(list_objects))
            .route("/{filename:.*}", web::get().to(get_object))
    })
    .bind((host, port))?
    .run()
    .await
}
