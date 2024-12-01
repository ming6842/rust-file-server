use actix_web::{web, App, HttpResponse, HttpServer, Responder, HttpRequest, http::header};
use chrono::Utc;
use serde::Serialize;
use serde_xml_rs::to_string;
use std::fs;
use std::path::PathBuf;
use log::{info, warn, error, debug};
use std::time::Instant;

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

struct AppState {
    storage_path: PathBuf,
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

fn get_client_info(req: &HttpRequest) -> String {
    let ip = req.peer_addr().map_or("unknown".to_string(), |addr| addr.to_string());
    let user_agent = req.headers()
        .get("user-agent")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("unknown");
    format!("Client IP: {}, User-Agent: {}", ip, user_agent)
}

async fn list_objects(req: HttpRequest, data: web::Data<AppState>) -> impl Responder {
    let start_time = Instant::now();
    info!("List objects request received from {}", get_client_info(&req));
    
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
        start_time.elapsed(),
        file_count,
        total_size
    );
    
    HttpResponse::Ok()
        .content_type("application/xml")
        .body(xml)
}

async fn get_object(req: HttpRequest, path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let start_time = Instant::now();
    let client_info = get_client_info(&req);
    info!("Get object request received for '{}' from {}", path, client_info);

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
    let etag = format!("\"{}\"", metadata.len());
    let last_modified = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    if let Some(range_header) = req.headers().get(header::RANGE) {
        if let Some(range_str) = range_header.to_str().ok() {
            debug!("Range header received: {}", range_str);
            
            if let Some(byte_range) = ByteRange::parse(range_str, file_size) {
                debug!("Parsed range: start={}, end={:?}", byte_range.start, byte_range.end);
                
                if let Ok(mut file) = fs::File::open(&file_path) {
                    use std::io::{Read, Seek, SeekFrom};
                    
                    if file.seek(SeekFrom::Start(byte_range.start)).is_ok() {
                        let end = byte_range.end.unwrap_or(file_size - 1);
                        let length = end - byte_range.start + 1;
                        let mut buffer = vec![0; length as usize];
                        
                        if file.read_exact(&mut buffer).is_ok() {
                            info!(
                                "Serving partial content for '{}': bytes {}-{}/{} to {}",
                                path, byte_range.start, end, file_size, client_info
                            );
                            
                            return HttpResponse::PartialContent()
                                .append_header(("Content-Range", format!("bytes {}-{}/{}", byte_range.start, end, file_size)))
                                .append_header(("Content-Length", length.to_string()))
                                .append_header(("ETag", etag))
                                .append_header(("Last-Modified", last_modified))
                                .append_header(("Accept-Ranges", "bytes"))
                                .append_header(("x-amz-request-id", format!("TX{}", Utc::now().timestamp())))
                                .body(buffer);
                        }
                    }
                }
            }
        }
        return HttpResponse::RangeNotSatisfiable()
            .append_header(("Content-Range", format!("bytes */{}", file_size)))
            .finish();
    }

    match fs::read(&file_path) {
        Ok(contents) => {
            info!(
                "File '{}' ({} bytes) served successfully to {} in {:?}",
                path,
                file_size,
                client_info,
                start_time.elapsed()
            );

            HttpResponse::Ok()
                .content_type("application/octet-stream")
                .append_header(("ETag", etag))
                .append_header(("Last-Modified", last_modified))
                .append_header(("Accept-Ranges", "bytes"))
                .append_header(("Content-Length", file_size.to_string()))
                .append_header(("x-amz-request-id", format!("TX{}", Utc::now().timestamp())))
                .body(contents)
        }
        Err(e) => {
            error!("Error reading file '{}': {}", path, e);
            HttpResponse::InternalServerError().finish()
        }
    }
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
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failed to create storage directory"));
    }

    // Get server IP and port from environment variables or use defaults
    let host = std::env::var("SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = std::env::var("SERVER_PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_address = format!("{}:{}", host, port);

    info!("⚠️  Server starting in PUBLIC mode - accepting connections from all interfaces");
    info!("🌐 Binding to {}", bind_address);
    info!("📂 Serving files from ./storage directory");

    // Log local IP addresses
    if let Ok(interfaces) = get_network_interfaces() {
        info!("Available network interfaces:");
        for (ip, kind) in interfaces {
            info!("  - {} ({})", ip, kind);
        }
    }

    if let Ok(entries) = fs::read_dir(&storage_path) {
        info!("Initial storage directory contents:");
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if let Ok(file_name) = entry.file_name().into_string() {
                    info!("  - {} ({} bytes)", file_name, metadata.len());
                }
            }
        }
    }

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                storage_path: storage_path.clone(),
            }))
            .wrap(actix_web::middleware::Logger::default())
            .route("/", web::get().to(list_objects))
            .route("/{filename:.*}", web::get().to(get_object))
    })
    .bind(&bind_address)?
    .run()
    .await
}

// Helper function to get network interfaces
fn get_network_interfaces() -> std::io::Result<Vec<(String, String)>> {
    use std::net::{IpAddr, Ipv4Addr};
    let mut interfaces = Vec::new();
    
    if let Ok(addrs) = std::net::ToSocketAddrs::to_socket_addrs(&("0.0.0.0:0", 0)) {
        for addr in addrs {
            let ip = addr.ip();
            let kind = match ip {
                IpAddr::V4(ip4) => {
                    if ip4.is_loopback() {
                        "loopback".to_string()
                    } else if ip4.is_private() {
                        "private".to_string()
                    } else {
                        "public".to_string()
                    }
                },
                IpAddr::V6(ip6) => {
                    if ip6.is_loopback() {
                        "loopback (IPv6)".to_string()
                    } else {
                        "IPv6".to_string()
                    }
                }
            };
            interfaces.push((ip.to_string(), kind));
        }
    }
    
    Ok(interfaces)
}