use tui::backend::Backend;
use tui::Frame;
use tui::widgets::TableState;
use crate::ui::Ui;

pub struct View<T> {
    state: T
}

impl <T> View<T>
    where T: Ui<T> {

}

pub enum Views {
    Welcome(View<Welcome>)
}

impl Ui<Views> for Views {
    fn ui(&self) {
        match state {
            Views::Welcome(v) => v.ui(f, &v.state)
        }
    }
}


impl <T> Ui<T> for View<T>
    where T: Ui<T>
{
    fn ui<B: Backend>(&self, f: &mut Frame<B>, state: &T) {
        self.state.ui(f, &self.state);
    }
}

pub struct Welcome {

}

impl View<Welcome> {
    pub fn new() -> Self {
        View {
            state: Welcome {

            }
        }
    }
}

struct Clusters {

}

impl View<Clusters> {

}


pub enum ErrorState {
    Error(String),
    None
}

pub struct AppState {
    error: ErrorState,
    pub(crate) view: Views
}

pub(crate) struct App {
    pub state: AppState,
}

impl App {
    pub(crate) fn new() -> App {
        App {
            state: AppState {
                view: Views::Welcome(View::new()),
                error: ErrorState::None
            },
        }
    }
}