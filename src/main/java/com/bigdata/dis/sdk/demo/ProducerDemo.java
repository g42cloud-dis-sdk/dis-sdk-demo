package com.bigdata.dis.sdk.demo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.g42cloud.dis.DIS;
import com.g42cloud.dis.DISClientBuilder;
import com.g42cloud.dis.core.util.StringUtils;
import com.g42cloud.dis.exception.DISClientException;
import com.g42cloud.dis.iface.data.request.PutRecordsRequest;
import com.g42cloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.g42cloud.dis.iface.data.response.PutRecordsResult;
import com.g42cloud.dis.iface.data.response.PutRecordsResultEntry;

public class ProducerDemo
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);
    
    public static void main(String args[])
    {
        runProduceDemo();
    }
    
    private static void runProduceDemo()
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
        String streamName = "stz_test_maxwell";
        
        // 配置上传的数据
        String message = "hello world.";
        
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<PutRecordsRequestEntry>();
        ByteBuffer buffer = ByteBuffer.wrap(message.getBytes());
        for (int i = 0; i < 3; i++)
        {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            putRecordsRequestEntry.setData(buffer);
            putRecordsRequestEntry.setPartitionKey(String.valueOf(ThreadLocalRandom.current().nextInt(1000000)));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        
        LOGGER.info("========== BEGIN PUT ============");
        
        PutRecordsResult putRecordsResult = null;
        try
        {
            putRecordsResult = dic.putRecords(putRecordsRequest);
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
        
        if (putRecordsResult != null)
        {
            LOGGER.info("Put {} records[{} successful / {} failed].",
                putRecordsResult.getRecords().size(),
                putRecordsResult.getRecords().size() - putRecordsResult.getFailedRecordCount().get(),
                putRecordsResult.getFailedRecordCount());
            
            for (int j = 0; j < putRecordsResult.getRecords().size(); j++)
            {
                PutRecordsResultEntry putRecordsRequestEntry = putRecordsResult.getRecords().get(j);
                if (!StringUtils.isNullOrEmpty(putRecordsRequestEntry.getErrorCode()))
                {
                    // 上传失败
                    LOGGER.error("[{}] put failed, errorCode [{}], errorMessage [{}]",
                        new String(putRecordsRequestEntryList.get(j).getData().array()),
                        putRecordsRequestEntry.getErrorCode(),
                        putRecordsRequestEntry.getErrorMessage());
                }
                else
                {
                    // 上传成功
                    LOGGER.info("[{}] put success, partitionId [{}], partitionKey [{}], sequenceNumber [{}]",
                        new String(putRecordsRequestEntryList.get(j).getData().array()),
                        putRecordsRequestEntry.getPartitionId(),
                        putRecordsRequestEntryList.get(j).getPartitionKey(),
                        putRecordsRequestEntry.getSequenceNumber());
                }
            }
        }
        LOGGER.info("========== END PUT ============");
    }
    
}
