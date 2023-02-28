package com.bigdata.dis.sdk.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.g42cloud.dis.DIS;
import com.g42cloud.dis.DISClientBuilder;
import com.g42cloud.dis.exception.DISClientException;
import com.g42cloud.dis.iface.data.request.GetPartitionCursorRequest;
import com.g42cloud.dis.iface.data.request.GetRecordsRequest;
import com.g42cloud.dis.iface.data.response.GetPartitionCursorResult;
import com.g42cloud.dis.iface.data.response.GetRecordsResult;
import com.g42cloud.dis.iface.data.response.Record;
import com.g42cloud.dis.util.PartitionCursorTypeEnum;

public class ConsumerDemo
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);
    
    public static void main(String args[])
    {
        runConsumerDemo();
    }
    
    private static void runConsumerDemo()
    {
        // 创建DIS客户端实例
        DIS dic = DISClientBuilder.standard()
            .withEndpoint("https://dis.cn-north-1.myg42cloud.com")
            .withAk("YOUR_AK")
            .withSk("YOUR_SK")
            .withProjectId("YOUR_PROJECT_ID")
            .withRegion("cn-north-1")
            .withDefaultClientCertAuthEnabled(true)
            .build();
        
        // 配置流名称
        String streamName = "stz_test";
        
        // 配置数据下载分区ID
        String partitionId = "0";
        
        // 配置下载数据序列号
        String startingSequenceNumber = "0";
        
        // 配置下载数据方式
        // AT_SEQUENCE_NUMBER: 从指定的sequenceNumber开始获取，需要设置GetPartitionCursorRequest.setStartingSequenceNumber
        // AFTER_SEQUENCE_NUMBER: 从指定的sequenceNumber之后开始获取，需要设置GetPartitionCursorRequest.setStartingSequenceNumber
        // TRIM_HORIZON: 从最旧的记录开始获取
        // LATEST: 从最新的记录开始获取
        // AT_TIMESTAMP: 从指定的时间戳(13位)开始获取，需要设置GetPartitionCursorRequest.setTimestamp
        String cursorType = PartitionCursorTypeEnum.AT_SEQUENCE_NUMBER.name();
        
        try
        {
            // 获取数据游标
            GetPartitionCursorRequest request = new GetPartitionCursorRequest();
            request.setStreamName(streamName);
            request.setPartitionId(partitionId);
            request.setCursorType(cursorType);
            request.setStartingSequenceNumber(startingSequenceNumber);
            GetPartitionCursorResult response = dic.getPartitionCursor(request);
            String cursor = response.getPartitionCursor();
            
            LOGGER.info("Get stream {}[partitionId={}] cursor success : {}", streamName, partitionId, cursor);
            
            GetRecordsRequest recordsRequest = new GetRecordsRequest();
            GetRecordsResult recordResponse = null;
            while (true)
            {
                recordsRequest.setPartitionCursor(cursor);
                recordResponse = dic.getRecords(recordsRequest);
                // 下一批数据游标
                cursor = recordResponse.getNextPartitionCursor();
                
                for (Record record : recordResponse.getRecords())
                {
                    LOGGER.info("Get Record [{}], partitionKey [{}], sequenceNumber [{}].",
                        new String(record.getData().array()),
                        record.getPartitionKey(),
                        record.getSequenceNumber());
                }
            }
        }
        catch (DISClientException e)
        {
            LOGGER.error("Failed to get a normal response, please check params and retry. Error message [{}]",
                e.getMessage(),
                e);
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
