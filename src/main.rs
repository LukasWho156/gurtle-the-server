use std::env;
use serde::{Serialize, Deserialize};
use mongodb::{bson::doc, Client, options::{ClientOptions, FindOptions}, Collection};
use actix_web::{get, post, web, App, HttpResponse, HttpServer};
use futures::stream::TryStreamExt;
use chrono::{Duration, offset::Utc};
use sha256;

#[derive(Serialize, Deserialize, Debug)]
struct Entry {
    name: String,
    score: i32,
    datetime: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct SubmittedEntry {
    name: String,
    score: i32,
    hash: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Position {
    position: u64,
}

async fn set_up_db(uri: &str) -> Result<Client, mongodb::error::Error> {
    let client_options = ClientOptions::parse(uri).await?;
    let client = Client::with_options(client_options)?;
    Ok(client)
}

#[get("/scores/{duration}")]
async fn get_scores(path: web::Path<String>, collection: web::Data<Collection<Entry>>) -> HttpResponse {
    let duration = path.into_inner();
    let now = Utc::now();
    let beginning = match duration.as_str() {
        "weekly" => now - Duration::weeks(1),
        "monthly" => now - Duration::weeks(4),
        _ => now,
    };
    let mut scores: Vec<Entry> = Vec::new();
    let filter = match duration.as_str() {
        "alltime" => doc! {},
        _ => doc! {
            "datetime": { "$gte": beginning.to_string() }
        },
    };
    let options = FindOptions::builder()
        .sort(doc! {"score": 1})
        .limit(10)
        .build();
    let mut cursor = collection.into_inner()
        .find(filter, options)
        .await.unwrap();
    while let Some(score) = cursor.try_next().await.unwrap() {
        scores.push(score);
    }
    HttpResponse::Ok().json(scores)
}

#[get("/position/{duration}/{score}")]
async fn get_position(path: web::Path<(String, i32)>, collection: web::Data<Collection<Entry>>) -> HttpResponse {
    let (duration, score) = path.into_inner();
    let now = Utc::now();
    let beginning = match duration.as_str() {
        "weekly" => now - Duration::weeks(1),
        "monthly" => now - Duration::weeks(4),
        _ => now,
    };
    let filter = match duration.as_str() {
        "alltime" => doc! { "score": {"$gte": score} },
        _ => doc! {
            "datetime": { "$gte": beginning.to_string() },
            "score": {"$gte": score}
        },
    };
    let position = collection.into_inner().count_documents(filter, None).await.unwrap() + 1;
    HttpResponse::Ok().json(Position { position })
}

#[post("/submitscore")]
async fn submit_score(collection: web::Data<Collection<Entry>>, submitted: web::Json<SubmittedEntry>) -> HttpResponse {
    let now = Utc::now();
    let submitted = submitted.into_inner();
    let value = sha256::digest(format!("{}TheTurtle{}", submitted.name, submitted.score));
    if value != submitted.hash {
        return HttpResponse::Forbidden().body("Score rejected: Invalid hash");
    }
    let data = Entry {
        name: submitted.name,
        score: submitted.score,
        datetime: now.to_string(),
    };
    let result = collection.into_inner().insert_one(data, None).await;
    match result {
        Ok(_) => HttpResponse::Ok().body("Score added"),
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let uri = env::var("MONGO_URI").unwrap_or(String::from("mongodb://localhost:27017"));
    let client = set_up_db(uri.as_str()).await.expect("Should be able to connect do Mongo DB");
    let db = client.database("gurtle");
    let collection = db.collection::<Entry>("scores");
    let port: u16 = env::var("PORT").unwrap_or(String::from("3000")).parse().unwrap_or(3000);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(collection.clone()))
            .service(get_scores)
            .service(get_position)
            .service(submit_score)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await

}