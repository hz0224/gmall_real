package handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import constant.GmallConstant;
import util.MyKafkaSender;

import java.util.List;

/**
 * Created by Liu HangZhou on 2020/03/31
 * desc: 根据不同的业务去处理Canal发来的数据
 */
public class CanalHandler {

    private List<CanalEntry.RowData> rowDatasList;  //行的集合
    private String tableName;
    private CanalEntry.EventType eventType;
    private String event;       //对哪张表执行了哪个操作.

    //对哪张执行了怎样的操作，影响了怎样的数据.
    public CanalHandler(List<CanalEntry.RowData> rowDatasList, String tableName, CanalEntry.EventType eventType) {
        this.rowDatasList = rowDatasList;
        this.tableName = tableName;
        this.eventType = eventType;
        this.event = eventType + "->" + tableName;
    }

    public void handleController(){
        System.out.println(event);
        switch (event){
            case "INSERT->order_info" : handleInsertOrderInfo();break;
            case "INSERT->order_detail": handleInsertOrderDetail();break;
            case "INSERT->user_info": handleInsertUserInfo();break;
            case "UPDATE->user_info": handleInsertUserInfo();break;     //修改时和新增处理逻辑相同
            default:
        }
    }

    //处理 user_info表的插入或修改操作
    private void handleInsertUserInfo() {
        sendKafka(GmallConstant.KAFKA_TOPIC_USER_INFO);
    }

    //处理 order_detail表的插入操作
    private void handleInsertOrderDetail() {
        sendKafka(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL);
    }

    //处理 order_info表的插入操作.
    private void handleInsertOrderInfo() {
        sendKafka(GmallConstant.KAFKA_TOPIC_ORDER);
    }

    //发送到kafka
    private void sendKafka(String topic){
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
            for (CanalEntry.Column column : columnsList) {
                String name = column.getName();
                String value = column.getValue();
                jsonObject.put(name,value);
            }
            //发送kafka
            MyKafkaSender.send(topic,jsonObject.toJSONString());
        }

    }
}
