package io.oyewale.oyelami.datalogi.service.consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class HBaseService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Configuration configuration;

    private final String tableName = "myDeviceData";

    private final String familyName = "logged-data";

    public HBaseService(@Qualifier("myConfig") Configuration configuration) {
        this.configuration = configuration;
    }

    @PostConstruct
    public void createTable() {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             Admin admin = connection.getAdmin()) {
            tableDescriptor.addFamily(new HColumnDescriptor(familyName));

            if (admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println("Table Exists");
                logger.info("Table:[" + tableName + "] Exists");
            } else {
                admin.createTable(tableDescriptor);
                System.out.println("Create table Successfully!!!Table Name:[" + tableName + "]");
                logger.info("Create table Successfully!!!Table Name:[" + tableName + "]");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
