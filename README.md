# Jindo Gateway

OSS-HDFS service (JindoFS service) is a cloud-native data lake storage service. It is fully compatible with the HDFS file system interface, meeting data lake computing scenarios in big data and AI fields. For more information, please refer to [JindoData](https://github.com/aliyun/alibabacloud-jindodata).

This project is an OSS-HDFS service proxy tool. It can receive open-source HDFS RPC requests, forward them to the OSS-HDFS server, receive the OSS-HDFS response, convert it into an open-source HDFS RPC response, and send it back to the client. Additionally, it proxies HDFS data stream requests, writing data to OSS or reading OSS files. Computing engines that cannot integrate JindoSDK can deploy this project locally, thereby accessing OSS-HDFS service just like accessing open-source HDFS.

## System Requirements

- Java 8 or higher
- Maven 3.6 or higher

## Build Project

```bash
# Clean and compile
mvn clean compile

# Run tests (set your configuration correctly in test/resources)
mvn test

# Build project
mvn clean install

# Build distribution package
mvn clean package -Pdist
```

The built distribution package is located at `target/jindo-gateway-{version}.tar.gz`.

## Start Service

First unpack the distribution package:

```bash
tar -xzf target/jindo-gateway-*.tar.gz -C /path/to/install
cd /path/to/install/jindo-gateway-*
```

### Configuration

```bash
# set your OSS-HDFS configuration
vim etc/jindo-gateway/core-site.xml

# set your HDFS configuration
vim etc/jindo-gateway/hdfs-site.xml
```

### Start NameNode

```bash
./bin/start-namenode.sh
```

### Start DataNode

```bash
./bin/start-datanode.sh
```

### Stop Service

```bash
# Stop NameNode
./bin/stop-namenode.sh

# Stop DataNode
./bin/stop-datanode.sh
```

## Configuration

Configuration files are located in `etc/jindo-gateway/` directory:

- `core-site.xml`: Core & OSS-HDFS configuration
- `hdfs-site.xml`: HDFS related configuration
- `log4j.properties`: Log configuration

Log files are stored in the following locations:
- NameNode Log: `$JINDO_GATEWAY_LOG_DIR/namenode.log`
- DataNode Log: `$JINDO_GATEWAY_LOG_DIR/datanode.log`

JINDO_GATEWAY_LOG_DIR = /tmp/jindo-gateway by default.

## Directory Structure

The directory structure after unpacking the distribution package is as follows:

```
jindo-gateway-{version}/
├── bin/                 # Start and stop scripts
├── etc/
│   └── jindo-gateway/   # Configuration files
└── lib/                 # Dependencies
    ├── jindo-gateway/   # Project jar files
    ├── hdfs/            # Hadoop related dependencies
    └── common/          # Other common dependencies
```

## Usage

1. Modify OSS access key and endpoint in configuration files.
2. Start NameNode service.
3. Start one or more DataNode services.
4. Use HDFS client to connect to the gateway.

## Notice

1. Not support hflush.
2. hsync only guarantees data durability, not metadata consistency. If your file output stream shutdown before close, although the whole data can be read, but the file size will be smaller than expected.

## License

Apache License 2.0
