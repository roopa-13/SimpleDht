package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class SimpleDhtDbHelper extends SQLiteOpenHelper {

    /*Below modified code is based on understanding
    https://developer.android.com/training/data-storage/sqlite*/

    //Database name
    private static final String DATABASE_NAME = "simpleDhtDb.db";
    //To track the database schema changes
    private static final int VERSION = 1;
    //Table Name
    public static final String TABLE_NAME = "simpleDht";
    private static final String TAG = SimpleDhtDbHelper.class.getName();

    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    SimpleDhtDbHelper(Context context){super(context, DATABASE_NAME, null, VERSION);
        Log.d(TAG, "Table "+ SimpleDhtDbHelper.TABLE_NAME+ "path "+
                context.getDatabasePath(DATABASE_NAME).getAbsolutePath());
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {

        final String CREATE_TABLE =
                "CREATE TABLE " + SimpleDhtDbHelper.TABLE_NAME + " (" +
                KEY_FIELD + " TEXT NOT NULL PRIMARY KEY, " +
                VALUE_FIELD + " TEXT NOT NULL);";

        sqLiteDatabase.execSQL(CREATE_TABLE);
        Log.i(TAG, "Table "+ SimpleDhtDbHelper.TABLE_NAME+ " created!");

    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int oldVersion, int newVersion) {
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS "+ SimpleDhtDbHelper.TABLE_NAME);
        onCreate(sqLiteDatabase);

        Log.i(TAG, "Table "+ SimpleDhtDbHelper.TABLE_NAME+ " deleted!");
    }
}
