use std::{error::Error, io};
use std::time::Duration;
use bytes::BytesMut;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use kafka_protocol::messages::{ApiKey, MetadataRequest, MetadataResponse, RequestHeader, RequestKind};
use kafka_protocol::protocol::{Decodable, StrBytes};
use tower::Service;
use tracing_subscriber::{EnvFilter, fmt};
use tracing_subscriber::layer::SubscriberExt;
use tui::{
    backend::{Backend, CrosstermBackend},
    Terminal,
};

mod ui;
mod kafka;
mod app;


#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // install tracing
    let subscriber = tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive(tracing::Level::TRACE.into())
        )
        .with(fmt::Layer::new().compact().with_writer(io::stdout));
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global collector");

    // let mut a = kafka::client::DisconnectedKafkaClient::new("127.0.0.1:9092".parse().unwrap());
    // let mut b = a.connect().await.unwrap();
    // let mut header = RequestHeader::default();
    // header.client_id = Some(StrBytes::from_str("hi"));
    // header.request_api_key = ApiKey::MetadataKey as i16;
    // header.request_api_version = 12;
    // let mut req = MetadataRequest::default();
    // req.topics = None;
    // req.allow_auto_topic_creation = true;
    // let foo = b.call((header, RequestKind::MetadataRequest(req))).await.unwrap();
    // println!("foo, {:?}", foo);

    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // create app and run it
    let app = app::App::new();
    let res = run_app(&mut terminal, app);

    // restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{:?}", err)
    }

    Ok(())
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, mut app: app::App) -> io::Result<()> {
    loop {
        terminal.draw(|f| ui::ui(f, &mut app))?;

        // if let Event::Key(key) = event::read()? {
        //     match key.code {
        //         KeyCode::Char('q') => return Ok(()),
        //         KeyCode::Down => app.next(),
        //         KeyCode::Up => app.previous(),
        //         _ => {}
        //     }
        // }
    }
}
