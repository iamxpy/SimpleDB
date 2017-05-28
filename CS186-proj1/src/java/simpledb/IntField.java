package simpledb;

import java.io.*;

/**
 * Instance of Field that stores a single integer.
 */
public class IntField implements Field {
    
	private static final long serialVersionUID = 1L;
	
	private int value;

    public int getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param i The value of this field.
     */
    public IntField(int i) {
        value = i;
    }

    public String toString() {
        return Integer.toString(value);
    }

    public int hashCode() {
        return value;
    }

    public boolean equals(Object field) {
        return ((IntField) field).value == value;
    }

    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(value);
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     *
     * @throws ClassCastException if val is not an IntField
     * @see Field#compare
     */
    public boolean compare(Predicate.Op op, Field val) {

        IntField iVal = (IntField) val;

        switch (op) {
        case EQUALS:
            return value == iVal.value;
        case NOT_EQUALS:
            return value != iVal.value;

        case GREATER_THAN:
            return value > iVal.value;

        case GREATER_THAN_OR_EQ:
            return value >= iVal.value;

        case LESS_THAN:
            return value < iVal.value;

        case LESS_THAN_OR_EQ:
            return value <= iVal.value;

    case LIKE:
        return value == iVal.value;
        }

        return false;
    }

    /**
     * Return the Type of this field.
     * @return Type.INT_TYPE
     */
	public Type getType() {
		return Type.INT_TYPE;
	}
}
