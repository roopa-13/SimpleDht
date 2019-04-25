package edu.buffalo.cse.cse486586.simpledht;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.IllegalCharsetNameException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.acl.LastOwnerException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.NetworkOnMainThreadException;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.apache.http.auth.NTUserPrincipal;

import static edu.buffalo.cse.cse486586.simpledht.SimpleDhtDbHelper.TABLE_NAME;

public class SimpleDhtProvider extends ContentProvider {

    static String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    private static final int SERVER_PORT = 10000;

    List<String> portList = Arrays.asList(REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4);
    private ArrayList<String> nodeList = new ArrayList<String>();

    private String portStr = null;
    private String myPort = null;
    private String predecessor = null;
    private String successor = null;

    private enum NodeState{JOIN, JOINFORWARD, INSERT, QUERY, QUERYFORWARD, DELETE, DELETEFORWARD};
    private  NodeState nodeState;

    private static final String TAG = SimpleDhtProvider.class.getName();

    private SimpleDhtDbHelper simpleDhtDbHelper;
    public static final Uri BASE_URI = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        final SQLiteDatabase db = simpleDhtDbHelper.getWritableDatabase();
        Socket socket;
        DataInputStream in;
        DataOutputStream out;

        String msgRcvd;
        nodeState = NodeState.DELETE;

        int deletedRows = 0;

           try {
               if (selection.equals("@")) {
                   Log.d(TAG, "Content Provider - delete() @ condition started");
                   deletedRows = db.delete(TABLE_NAME, null, null);
               } else if (selection.contains("*")) {
                   if (successor == null && predecessor == null) {
                       deletedRows = db.delete(TABLE_NAME, null, null);
                   } else if (successor != null) {
                       socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                       out = new DataOutputStream(socket.getOutputStream());

                       out.writeUTF(nodeState + ":" + myPort);
                       out.flush();

                   }
               }else if (selection.trim().length() == 5) {
                       deletedRows = db.delete(TABLE_NAME, null, null);

                       if (successor != selection) {
                           socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                           out = new DataOutputStream(socket.getOutputStream());

                           out.writeUTF(nodeState + ":" + myPort);
                           out.flush();
                       }
               } else if(selection.contains(":")){
                   String[] split = selection.split(":");
                   nodeState = NodeState.DELETEFORWARD;

                   String key = split[1].trim();

                   deletedRows = db.delete(TABLE_NAME, "key='"+key+"'", null);
                   if(deletedRows == 0){
                       if(successor != null){
                           if(successor != split[0].trim()) {
                               socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                               out = new DataOutputStream(socket.getOutputStream());

                               out.writeUTF(nodeState + ":" + split[0].trim() + ":" + key);
                               out.flush();
                           }

                       }
                   }

               }else{
                   String key = selection;
                   nodeState = NodeState.DELETEFORWARD;

                   deletedRows = db.delete(TABLE_NAME, "key='"+key+"'", null);

                   if(deletedRows == 0){
                       if(successor != null){
                           socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                           out = new DataOutputStream(socket.getOutputStream());

                           out.writeUTF(nodeState + ":" + myPort + ":" + key);
                           out.flush();

                       }
                   }
               }
           } catch (UnknownHostException e) {
               e.printStackTrace();
           } catch (IOException e) {
               e.printStackTrace();
           }

        return deletedRows;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        final SQLiteDatabase db = simpleDhtDbHelper.getWritableDatabase();

        Uri returnUri = null;
        String key = values.getAsString(SimpleDhtDbHelper.KEY_FIELD);
        String value = values.getAsString(SimpleDhtDbHelper.VALUE_FIELD);

        nodeState = NodeState.INSERT;

        try {
            if ((predecessor == null && successor == null) ||
                    (genHash(key).compareTo(genHash(String.valueOf(Integer.parseInt(predecessor) /2))) > 0 &&
                            genHash(key).compareTo(genHash(String.valueOf(Integer.parseInt(myPort) / 2))) <= 0) ||
                    (genHash(String.valueOf(Integer.parseInt(predecessor)/2)).compareTo(genHash(String.valueOf(Integer.parseInt(myPort)/2))) > 0  &&
                            (genHash(key).compareTo(genHash(String.valueOf(Integer.parseInt(predecessor) /2))) > 0 ||
                            genHash(key).compareTo(genHash(String.valueOf(Integer.parseInt(myPort) / 2))) <= 0))) {

                long id = db.replace(TABLE_NAME, null, values);

                Log.d(TAG, "Content Provider - insert() key-value" + key + "-" + value + " inserted in port " + Integer.parseInt(myPort) / 2);
                if (id > 0) {
                    returnUri = ContentUris.withAppendedId(BASE_URI, id);
                } else {
                    throw new android.database.SQLException("Failed to insert row into " + uri);
                }

            } else {
                Log.d(TAG, "Check : key hash : " + genHash(key));
                Log.d(TAG, "Check : pred hash : " + genHash(String.valueOf(Integer.parseInt(predecessor) /2)));
                Log.d(TAG, "Check : myPort hash : " + genHash(String.valueOf(Integer.parseInt(myPort) /2)));
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());

                String msgToSend = nodeState + ":" + key + "-" + value;
                Log.d(TAG, "Content Provider - insert() Message " + msgToSend + "  forwarded to successor " + successor);
                out.writeUTF(msgToSend);
                out.flush();

            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.v("insert", values.toString());
        return returnUri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        Context context = this.getContext();
        simpleDhtDbHelper = new SimpleDhtDbHelper(context);

        TelephonyManager tel =  (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf(Integer.parseInt(portStr) * 2);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            if(!portStr.equals("5554")){
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
            }else nodeList.add(myPort);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket clientSocket = sockets[0];
            Socket socket;
            DataInputStream in;
            DataOutputStream out;

            //Common variables
            String predSuccValues;
            String[] predSuccSplit;
            String initialNode;
            String key = null;
            String value = null;


            while (true) {
                try{
                    socket = clientSocket.accept();

                    in = new DataInputStream(socket.getInputStream());
                    String msg = in.readUTF();
                    String[] msgRcvd = msg.split(":"); //JOIN:myPort

                    nodeState = NodeState.valueOf(msgRcvd[0]);

                    switch (nodeState){
                        case JOIN:

                            Log.d(TAG, "Executing case "+nodeState);
                            if(!msgRcvd[1].equals(REMOTE_PORT0)){
                                nodeList.add(msgRcvd[1]);
                            }

                            HashMap<String, String> hashMap_join = new HashMap<String, String>();
                            ArrayList<String> hashValues = new ArrayList<String>(); //hashValues
                            ArrayList<String> sortedList = new ArrayList<String>(); //portNo 111**

                            for(String s: nodeList){
                                String val = String.valueOf(Integer.parseInt(s) / 2);
                                hashValues.add(genHash(val));
                                hashMap_join.put(genHash(val), s);
                            }
                            Collections.sort(hashValues);

                            for(String h: hashValues){
                                String portNo = hashMap_join.get(h);
                                sortedList.add(portNo);
                            }

                            Log.d(TAG, "Executing case "+nodeState + " Sorted List: " + sortedList);

                            predSuccValues =  getPredSuccValues(hashValues, genHash(portStr));
                            predSuccSplit = predSuccValues.trim().split("-");

                            for (String s : portList) { //Node list with 11108 - 11124
                                int port = Integer.parseInt(s) / 2;
                                String hashValue = genHash(String.valueOf(port)).trim();
                                if(hashValue.compareTo(predSuccSplit[0].trim())==0) {
                                    predecessor = s;
                                }
                                if(hashValue.compareTo(predSuccSplit[1].trim()) == 0) {
                                    successor = s;
                                }
                            }

                            Log.d(TAG, "Myport: " + myPort + " Predecessor: " + predecessor + " Successor: " + successor);

                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                            out = new DataOutputStream(socket.getOutputStream());

                            nodeState = NodeState.JOINFORWARD;

                            out.writeUTF(nodeState + ":" + sortedList.toString().substring(1, sortedList.toString().length() - 1));
                            out.flush();
                            break;

                        case JOINFORWARD:

                            Log.d(TAG, "Executing case "+nodeState);

                            String nodes = msgRcvd[1];
                            String[] nList = nodes.split(",");

                            Log.d(TAG, "Executing case "+nodeState + " Forwarded Sorted List: " + nodes);

                            for(int i=0; i<nList.length; i++) {
                                    if(nList[i].trim().equals(myPort)){
                                        if (i == 0) {
                                            predecessor = nList[nList.length - 1];
                                            successor = nList[i+1];
                                        } else if (i == nList.length - 1) {
                                            predecessor = nList[nList.length - 2];
                                            successor = nList[0];
                                        } else {
                                            predecessor = nList[i -1];
                                            successor = nList[i + 1];
                                        }
                                    }
                                }

                            predecessor = predecessor.trim();
                            successor = successor.trim();

                            Log.d(TAG, "Myport: " + myPort + " Predecessor: " + predecessor + " Successor: " + successor);

                            if(!successor.equals(REMOTE_PORT0)){
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                                out = new DataOutputStream(socket.getOutputStream());

                                nodeState = NodeState.JOINFORWARD;

                                out.writeUTF(nodeState + ":" + nodes);
                                out.flush();
                            }
                            break;
                        case INSERT:

                            Log.d(TAG, "Executing case "+nodeState);

                            String[] input = msgRcvd[1].split("-");

                            ContentValues keyValue = new ContentValues();
                            keyValue.put(SimpleDhtDbHelper.KEY_FIELD, input[0].trim());
                            keyValue.put(SimpleDhtDbHelper.VALUE_FIELD, input[1].trim());

                            insert(BASE_URI, keyValue);
                            break;

                        case QUERY:
                            initialNode = msgRcvd[1];
                            String keyValuePair = "";

                            Log.d(TAG, "Executing " + nodeState + " inital Node value " + initialNode);

                            Cursor cursor = query(BASE_URI, null, "*", null, initialNode.trim());
                            while (cursor.moveToNext()) {
                                key = cursor.getString(cursor.getColumnIndex(SimpleDhtDbHelper.KEY_FIELD));
                                value = cursor.getString(cursor.getColumnIndex(SimpleDhtDbHelper.VALUE_FIELD));

                                keyValuePair += key + "-" + value + ";";
                            }

                            Log.d(TAG, "Keyvalue - pair -(1) " + keyValuePair);
                            out = new DataOutputStream(socket.getOutputStream());
                            out.writeUTF(keyValuePair);
                            out.flush();
                            break;

                        case QUERYFORWARD:
                            initialNode = msgRcvd[1];
                            String serach_key = msgRcvd[2];

                            Log.d(TAG, "Executing " + nodeState + " querying key " + serach_key);

                            Cursor cursor1 = query(BASE_URI, null, serach_key.trim(), null, initialNode.trim());
                            while (cursor1.moveToNext()) {
                                key = cursor1.getString(cursor1.getColumnIndex(SimpleDhtDbHelper.KEY_FIELD));
                                value = cursor1.getString(cursor1.getColumnIndex(SimpleDhtDbHelper.VALUE_FIELD));
                            }
                            out = new DataOutputStream(socket.getOutputStream());
                            Thread.sleep(20);
                            out.writeUTF(key + "-" + value);
                            out.flush();
                            break;

                        case DELETE:

                            delete(BASE_URI, msgRcvd[1], null);
                            break;

                        case DELETEFORWARD:
                            Log.d(TAG, "MESSAGE RECIEVED : " + msg);
                            delete(BASE_URI, msgRcvd[1].trim() + ":" + msgRcvd[2].trim(), null);
                            break;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msg) {
            String msgRcvd = msg[0];
            Socket socket;
            DataInputStream in;
            DataOutputStream out;

            nodeState = NodeState.JOIN;
            try {
                if (null != msgRcvd && !msgRcvd.isEmpty()) {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));
                    out = new DataOutputStream(socket.getOutputStream());

                    out.writeUTF(nodeState + ":" + myPort); // JOIN:111**
                    out.flush();
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        Cursor cursor = null;
        final SQLiteDatabase db = simpleDhtDbHelper.getWritableDatabase();
        String keyValuePair = "";
        String result = null;
        String msgRcvd = null;
        nodeState = NodeState.QUERY;

        Socket socket;
        DataOutputStream out;
        DataInputStream in;

        try{
            if(selection.equals("@")){
                cursor = db.query(SimpleDhtDbHelper.TABLE_NAME, null, null, null, null, null, null);
            }else if(selection.equals("*")) {

                if (successor == null && predecessor == null) {
                    cursor = db.query(SimpleDhtDbHelper.TABLE_NAME, null, null, null, null, null, null);
                } else if (successor != null) {
                        cursor = db.query(SimpleDhtDbHelper.TABLE_NAME, null, null, null, null, null, null);

                        Log.d(TAG, "Query Row count: " + cursor.getCount() + "for myPort " + myPort);
                        while (cursor.moveToNext()) {
                            String key = cursor.getString(cursor.getColumnIndex(SimpleDhtDbHelper.KEY_FIELD));
                            String value = cursor.getString(cursor.getColumnIndex(SimpleDhtDbHelper.VALUE_FIELD));

                            keyValuePair += key + "-" + value + ";";
                        }

                        Log.d(TAG, "Query " + selection + " request forwarded to successor " + successor + " from myPort: " + myPort);

                        if (null != sortOrder) {
                            if(!successor.equals(sortOrder.trim())) {

                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                                out = new DataOutputStream(socket.getOutputStream());

                                out.writeUTF(nodeState + ":" + sortOrder);
                                out.flush();
                                Log.d(TAG, "Message sent to the successor " + successor + " > " + nodeState + ":" + sortOrder + "(1)");

                                in = new DataInputStream(socket.getInputStream());
                                msgRcvd = in.readUTF();
                            }
                        } else {

                            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                            out = new DataOutputStream(socket.getOutputStream());

                            out.writeUTF(nodeState + ":" + myPort);
                            out.flush();
                            Log.d(TAG, "Message sent to the successor " + successor + " > " + nodeState + ":" + myPort + "(2)");

                            in = new DataInputStream(socket.getInputStream());
                            msgRcvd = in.readUTF();
                        }

                        Log.d(TAG, "Message receieved from the query forward: " + msgRcvd + " from " + successor);

                        if (msgRcvd == null || msgRcvd.isEmpty()) {
                            result = keyValuePair;
                        }else if(keyValuePair == null || keyValuePair.isEmpty()){
                            result = msgRcvd;
                        }else if((msgRcvd == null || msgRcvd.isEmpty()) && (keyValuePair == null || keyValuePair.isEmpty())){
                            result = "";
                        }else {
                            result = keyValuePair + ";" + msgRcvd;
                        }


                        Log.d(TAG, "Final Query * Result: " + result);
                        String[] keyValue = result.trim().split(";");

                        Log.d(TAG, "Key Value pair " + keyValue.toString());

                        MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDhtDbHelper.KEY_FIELD, SimpleDhtDbHelper.VALUE_FIELD});
                        if(!result.isEmpty()) {
                            for (String kv : keyValue) {
                                if(!kv.isEmpty()) {
                                    String[] kPair = kv.trim().split("-");
                                    Log.d(TAG, "KV pair " + kv);
                                    matrixCursor.addRow(new String[]{kPair[0].trim(), kPair[1].trim()});
                                }
                            }
                        }

                        cursor = matrixCursor;

                        Log.d(TAG, "Final Query Result RowCount: " + cursor.getCount());
                }
            }else{
                String key = selection;
                nodeState = NodeState.QUERYFORWARD;

                Log.d(TAG, "Searching for the key: " + key + " in myPort " + myPort);
                if(successor != null && sortOrder != myPort){
                    cursor = db.query(SimpleDhtDbHelper.TABLE_NAME, null, "key='"+selection+"'", null, null,null,null);

                    int rowCount = cursor.getCount();
                    Log.d(TAG,"Row Count for myPort: " + myPort + " is " + rowCount + " for key " + key);

                    if(rowCount == 0) {
                        Log.d(TAG, "Searching for the key: " + key + " in successor " + successor);
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor));
                        out = new DataOutputStream(socket.getOutputStream());

                        if (sortOrder != null) {
                            out.writeUTF(nodeState + ":" + sortOrder + ":" + key + " ");
                            out.flush();
//                            Log.d(TAG, "Message sent for the key : " + key + "to the successor " + successor + " > " + nodeState + ":" + sortOrder + ":" + key);

                        } else {
                            out.writeUTF(nodeState + ":" + myPort + ":" + key + " ");
                            out.flush();
//                            Log.d(TAG, "Message sent for the key : " + key + "to the successor " + successor + " > " + nodeState + ":" + myPort + ":" + key);
                        }

                        in = new DataInputStream(socket.getInputStream());
                        msgRcvd = in.readUTF();

                        Log.d(TAG, "Querying key: " + selection + " message returned " + msgRcvd);

                        MatrixCursor matrixCursor = new MatrixCursor(new String[] {SimpleDhtDbHelper.KEY_FIELD, SimpleDhtDbHelper.VALUE_FIELD});
                        String[] kPair = msgRcvd.split("-");
                        matrixCursor.addRow(new String[]{kPair[0].trim(), kPair[1].trim()});

                        cursor = matrixCursor;
                    }

                }else if(successor == null){
                        cursor = db.query(SimpleDhtDbHelper.TABLE_NAME, null, "key='"+selection+"'", null, null,null,null);
                    }

                }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String getPredSuccValues(ArrayList<String> nList, String clientPort) {
        for(int i=0; i<nList.size(); i++) {
            if (nList.get(i).trim().equals(clientPort.trim())) {
                if (i == 0) {
                    predecessor = nList.get(nList.size() - 1);
                    successor = nList.get(i + 1);
                } else if (i == nList.size() - 1) {
                    predecessor = nList.get(i - 1);
                    successor = nList.get(0);
                } else {
                    predecessor = nList.get(i - 1);
                    successor = nList.get(i + 1);
                }
            }
        }
        String ps_value = predecessor + "-" + successor;
        Log.d(TAG, "Node List: " + nList);
        Log.d(TAG,"getPredSuccValues() "+" clientPort: " + clientPort + " predecessor and successor string message: " + predecessor + " " + successor);
        return ps_value.trim();
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
