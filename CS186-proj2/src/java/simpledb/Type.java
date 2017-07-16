package simpledb;

import java.text.ParseException;
import java.io.*;

/**
 * Class representing a type in SimpleDB.
 * Types are static objects defined by this class; hence, the Type
 * constructor is private.
 */
public enum Type implements Serializable {

    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                return new IntField(dis.readInt());
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }
        @Override
        public String toString(){
            return "INT";
        }

    }, STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN + 4;
        }

        /**
         *
         * @param dis The input stream to read from
         * @return 解析得到的Field
         * @throws ParseException
         *
         * @see StringField serialize()方法是将一个StringField写入文件的过程，有助于理解parse(从文件读入的过程)
         */
        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                int strLen = dis.readInt();
                byte bs[] = new byte[strLen];
                dis.read(bs);
                dis.skipBytes(STRING_LEN - strLen);
                return new StringField(new String(bs), STRING_LEN);
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }
        @Override
        public String toString(){
            return "STRING";
        }
    };

    public static final int STRING_LEN = 128;

    /**
     * @return the number of bytes required to store a field of this type.
     */
    public abstract int getLen();

    /**
     * @param dis The input stream to read from
     * @return a Field object of the same type as this object that has contents
     * read from the specified DataInputStream.
     * @throws ParseException if the data read from the input stream is not
     *                        of the appropriate type.
     */
    public abstract Field parse(DataInputStream dis) throws ParseException;

}
