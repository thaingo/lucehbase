package ch.ymc.lucehbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;

public class HBaseUtils {

    public static final String keySpace            = "Lucandra";
    public static final byte[] termVecColumnFamily = "TermVectors".getBytes();
    public static final byte[] docColumnFamily     = "Documents".getBytes();
    public static final String delimeter           = ""+new Character((char)255)+new Character((char)255);
    public static final byte[] documentIdField     = (delimeter+"KEY"+delimeter).getBytes();
    public static final byte[] documentMetaField   = (delimeter+"META"+delimeter).getBytes();
    
    private static final Logger logger = Logger.getLogger(HBaseUtils.class);

//    public static Cassandra.Client createConnection() throws TTransportException {
//        
//        
//        if(System.getProperty("cassandra.host") == null || System.getProperty("cassandra.port") == null) {
//           logger.warn("cassandra.host or cassandra.port is not defined, using default");
//        }
//        
//        //connect to cassandra
//        TSocket socket = new TSocket(
//                System.getProperty("cassandra.host","localhost"), 
//                Integer.valueOf(System.getProperty("cassandra.port","9160")));
//        
//        
//        TTransport trans;
//        
//        if(Boolean.valueOf(System.getProperty("cassandra.framed", "false")))
//            trans = new TFramedTransport(socket);
//        else
//            trans = socket;
//        
//        trans.open();
//        TProtocol protocol = new TBinaryProtocol(trans);
//
//        return new Cassandra.Client(protocol);
//    }

    public static String createColumnName(Term term) {
        return createColumnName(term.field(), term.text());
    }

    public static String createColumnName(String field, String text) {
        return field + delimeter + text;
    }

    public static Term parseTerm(byte[] termStr) {
        String[] parts = null;

              
        try {
            parts = new String(termStr,"UTF-8").split(delimeter);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
       

        if (parts == null || parts.length != 2) {
            throw new RuntimeException("invalid term format: " + parts[0]);
        }

        return new Term(parts[0].intern(), parts[1]);
    }

    public static final byte[] intToByteArray(int value) {
        return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
    }

    public static final int byteArrayToInt(byte[] b) {
        return (b[0] << 24) + ((b[1] & 0xFF) << 16) + ((b[2] & 0xFF) << 8) + (b[3] & 0xFF);
    }

    public static final byte[] intVectorToByteArray(List<Integer> intVector) {
        ByteBuffer buffer = ByteBuffer.allocate(4 * intVector.size());

        for (int i : intVector) {
            buffer.putInt(i);
        }

        return buffer.array();
    }

    public static boolean compareByteArrays(byte[] a, byte[] b) {

        if (a.length != b.length)
            return false;

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i])
                return false;
        }

        return true;

    }

    public static final int[] byteArrayToIntArray(byte[] b) {

        if (b.length % 4 != 0)
            throw new RuntimeException("Not a valid int array:" + b.length);

        int[] intArray = new int[b.length / 4];
        int idx = 0;

        for (int i = 0; i < b.length; i += 4) {
            intArray[idx] = (b[i] << 24) + ((b[i + 1] & 0xFF) << 16) + ((b[i + 2] & 0xFF) << 8) + (b[i + 3] & 0xFF);
        }

        return intArray;
    }

    public static final byte[] encodeLong(long l) {
        ByteBuffer buffer = ByteBuffer.allocate(8);

        buffer.putLong(l);

        return buffer.array();
    }

    public static final long decodeLong(byte[] bytes) {

        if (bytes.length != 8)
            throw new RuntimeException("must be 8 bytes");

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        return buffer.getLong();
    }

    public static final byte[] encodeUUID(UUID docUUID) {

        ByteBuffer buffer = ByteBuffer.allocate(16);

        buffer.putLong(docUUID.getMostSignificantBits());
        buffer.putLong(docUUID.getLeastSignificantBits());
        return buffer.array();
    }

    public static final UUID readUUID(byte[] bytes) {

        if (bytes.length != 16)
            throw new RuntimeException("uuid must be exactly 16 bytes");

        return UUID.nameUUIDFromBytes(bytes);

    }
   
    /** Read the object from bytes string. */
    public static Object fromBytes(byte[] data) throws IOException,
            ClassNotFoundException {
 
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
        Object o = ois.readObject();
        ois.close();
        return o;
    }
 
    /** Write the object to bytes. */
    public static byte[] toBytes(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return baos.toByteArray();
    }
}
