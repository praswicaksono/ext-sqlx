use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;
use php_tokio::EventLoop;
use crate::types::{SharedColumns, RowValues, StreamRow, build_zval_from_values};

#[php_class]
#[php(name = "Sqlx\\QueryIterator", implements(ce = ext_php_rs::zend::ce::iterator, stub = "\\Iterator"))]
pub struct QueryIterator {
    pub receiver: Option<tokio::sync::mpsc::Receiver<StreamRow>>,
    pub current_key: i64,
    pub current_val: Option<(SharedColumns, RowValues)>,
    pub is_valid: bool,
}

#[php_impl]
impl QueryIterator {
    pub fn current(&self) -> Zval {
        if let Some(val) = &self.current_val {
            match build_zval_from_values(val.0.clone(), val.1.clone()) {
                Ok(z) => z,
                Err(_) => {
                    let mut z = Zval::new();
                    z.set_null();
                    z
                }
            }
        } else {
            let mut z = Zval::new();
            z.set_null();
            z
        }
    }
    
    pub fn key(&self) -> i64 {
        self.current_key
    }
    
    pub fn next(&mut self) -> crate::types::SqlxResult<()> {
        self.current_key += 1;
        self.fetch_next()
    }
    
    pub fn rewind(&mut self) -> crate::types::SqlxResult<()> {
        self.current_key = 0;
        self.fetch_next()
    }
    
    pub fn valid(&self) -> bool {
        self.is_valid
    }
}

impl QueryIterator {
    fn fetch_next(&mut self) -> crate::types::SqlxResult<()> {
        if let Some(mut rx) = self.receiver.take() {
            let (res, rx_back) = EventLoop::suspend_on(async move {
                let r = rx.recv().await;
                (r, rx)
            });
            
            match res {
                Some(Ok(row)) => {
                    self.current_val = Some(row);
                    self.is_valid = true;
                    self.receiver = Some(rx_back);
                    Ok(())
                },
                Some(Err(e)) => {
                    self.is_valid = false;
                    Err(crate::types::SqlxError(e.to_string()))
                },
                None => {
                    self.is_valid = false;
                    self.receiver = None;
                    Ok(())
                }
            }
        } else {
            self.is_valid = false;
            Ok(())
        }
    }
}
