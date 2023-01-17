use tui::{
    backend::Backend,
    Frame,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style}, widgets::{Block, Borders, Cell, Row, Table},
};
use tui::layout::Direction::Vertical;

use crate::app;
use crate::app::{View, Welcome};

pub trait Ui<T, W> {
    fn ui<B: Backend>(&self, f: &mut Frame<B>, state: &T) -> W;
}

impl Ui<Welcome> for Welcome {
    fn ui<B: Backend>(&self, f: &mut Frame<B>, state: &Welcome) -> {

    }
}

pub(crate) fn ui<B: Backend>(f: &mut Frame<B>, app: &mut app::App) {
    let layout = Layout::default()
        .direction(Vertical)
        .constraints([Constraint::Percentage(100), Constraint::Min(10)])
        .split(f.size());
    let block = Block::default().title("Welcome").borders(Borders::ALL);
    f.render_widget(block, layout[0]);

    let v = &app.state.view;
    v.ui(&v);
}