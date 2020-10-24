pub struct File {
    pub name: String,
    pub path: String,
    pub bytes: u64,
    pub modified: u32
} 

pub struct Dir {
    pub name: String,
    pub path: String
}

pub enum Record {
    File,
    Dir
}

pub enum Table {
    CurrentDir
}