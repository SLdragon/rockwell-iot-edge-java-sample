package unittest.com.filtermodule;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import com.filtermodule.Filter;
import com.microsoft.azure.sdk.iot.device.Message;

import org.junit.Assert;
import org.junit.Test;

public class FilterModuleTest {
    @Test
    public void testFilterLessThanThreshold() {
        Filter filter = new Filter(25);
        Message source = createMessaage(25 - 1);
        Message result = filter.filterMessage(source);
        Assert.assertNull(result);
    }

    @Test
    public void testFilterLargerThanThreshold() {
        Filter filter = new Filter(25);
        Message source = createMessaage(25 + 1);
        Message result = filter.filterMessage(source);
        Assert.assertEquals("Alert", result.getProperty("MessageType"));
    }

    @Test
    public void testFilterLargerThanThresholdAndCopyAdditionalProperty() {
        String expected = "customTestValue";
        Filter filter = new Filter(25);
        Message source = createMessaage(25 + 1);
        source.setProperty("customTestKey", expected);
        Message result = filter.filterMessage(source);
        Assert.assertEquals(expected, result.getProperty("customTestKey"));
    }

    private Message createMessaage(int temperature) {
        Date date = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
        String strDate = dateFormat.format(date);
        String messageStr = String.format(
                "{\"machine\":{\"temperature\":%s,\"pressure\":0}, \"ambient\":{\"temperature\":0,\"humidity\":0},\"timeCreated\":\"%s\"}",
                Integer.toString(temperature), strDate);
        Message message = new Message(messageStr);
        return message;
    }
}