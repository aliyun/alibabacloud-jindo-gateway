package com.aliyun.jindodata.gateway.hdfs.protocol;

import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;

import java.io.IOException;

public interface JindoNamenodeProtocols extends ClientProtocol, DatanodeProtocol {
}
