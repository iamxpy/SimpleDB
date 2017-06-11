package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId pageId;

    private int tupleNum;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid     the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        pageId = pid;
        tupleNum = tupleno;
    }

    /**
     * tupleNum指的是tuple在其对应的page上的slot的位置吧
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return tupleNum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
            return true;
        } else if (o instanceof RecordId) {
            RecordId another = (RecordId) o;
            return pageId.equals(another.getPageId())
                    && another.tupleno() == tupleNum;
        } else return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        int result = 31 * pageId.hashCode() + tupleNum;
        return result;
    }
}
