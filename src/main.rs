use vectorkv::engine;
use env_logger;


fn main() {
    env_logger::init();
    engine::init_engine();
}
