package client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import constant.GmallConstant;
import handler.CanalHandler;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Created by Liu HangZhou on 2020/03/31
 * desc: canal客户端
 */
public class CanalClient {
    public static void main(String[] args) {

        //创建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hangzhou1-yun",11111),
                GmallConstant.CANAL_TOPIC,
                GmallConstant.CANAL_USER_NAME,
                GmallConstant.CANAL_PASSWORD);

        //建立连接
        canalConnector.connect();
        //监控 gmall_real库下所有的表
        canalConnector.subscribe("gmall_real.*");
        //接着上次读.
        canalConnector.rollback();

        //持续监控
        while (true){
            Message message = canalConnector.get(100);  //抓取最近100条sql影响的数据.
            //如果没有抓到数据,说明此时访问少，可以睡一会.
            if(message.getEntries().size() == 0){
                System.out.println("没有数据，休息一会");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //只有 entry的类型是 ROWDATA类型时才处理.
                    if(!entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) continue;

                    CanalEntry.RowChange rowChange = null;
                    //取出entry中的值
                    ByteString storeValue = entry.getStoreValue();
                    //反序列化
                    try {
                        rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }

                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                    //执行的sql类型
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    //具体到表
                    String tableName = entry.getHeader().getTableName();
                    CanalHandler canalHandler = new CanalHandler(rowDatasList, tableName, eventType);
                    canalHandler.handleController();
                }

            }

        }

    }
}
