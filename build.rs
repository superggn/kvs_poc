fn main() {
    // 普通生成 .rs 文件
    // prost_build::compile_protos(&["src/items.proto"], &["src"]).unwrap();
    // 定制生成 .rs 文件 => 给 struct 自动 derive traits
    let mut config = prost_build::Config::new();
    // config.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    config.type_attribute(".", "#[derive(PartialOrd)]");
    config
        .compile_protos(&["src/pb/abi.proto"], &["src"])
        .unwrap();
}
