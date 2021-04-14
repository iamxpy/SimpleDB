package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by orm on 10/7/15.
 */
public class BTreeChecker {

    /**
     * This class is only used for error-checking code.
     */
    static class SubtreeSummary {
        public int depth;
        public BTreePageId ptrLeft;
        public BTreePageId leftmostId;
        public BTreePageId ptrRight;
        public BTreePageId rightmostId;

        SubtreeSummary() {}

        SubtreeSummary(BTreeLeafPage base, int depth) {
            this.depth = depth;

            this.leftmostId = base.getId();
            this.rightmostId = base.getId();

            this.ptrLeft = base.getLeftSiblingId();
            this.ptrRight = base.getRightSiblingId();
        }

        static SubtreeSummary checkAndMerge(SubtreeSummary accleft, SubtreeSummary right) {
            assert(accleft.depth == right.depth);
            assert(accleft.ptrRight.equals(right.leftmostId));
            assert(accleft.rightmostId.equals(right.ptrLeft));

            SubtreeSummary ans = new SubtreeSummary();
            ans.depth = accleft.depth;

            ans.ptrLeft = accleft.ptrLeft;
            ans.leftmostId = accleft.leftmostId;

            ans.ptrRight = right.ptrRight;
            ans.rightmostId = right.rightmostId;
            return ans;
        }
    }

    /**
     * checks the integrity of the tree:
     * 1) parent pointers.
     * 2) sibling pointers.
     * 3) range invariants.
     * 4) record to page pointers.
     * 5) occupancy invariants. (if enabled)
     */
    public static void checkRep(BTreeFile bt, TransactionId tid, HashMap<PageId, Page> dirtypages,
                                boolean checkOccupancy) throws
            DbException, IOException, TransactionAbortedException {
        BTreeRootPtrPage rtptr = bt.getRootPtrPage(tid, dirtypages);

        if (rtptr.getRootId() == null) { // non existent root is a legal state.
            return;
        } else {
            SubtreeSummary res = checkSubTree(bt, tid, dirtypages,
                    rtptr.getRootId(), null, null, rtptr.getId(), checkOccupancy, 0);
            assert (res.ptrLeft == null);
            assert (res.ptrRight == null);
        }
    }

    static SubtreeSummary checkSubTree(BTreeFile bt, TransactionId tid, HashMap<PageId, Page> dirtypages,
                                       BTreePageId pageId, Field lowerBound, Field upperBound,
                                       BTreePageId parentId, boolean checkOccupancy, int depth) throws
            TransactionAbortedException, DbException {
        BTreePage page = (BTreePage )bt.getPage(tid, dirtypages, pageId, Permissions.READ_ONLY);
        assert(page.getParentId().equals(parentId));

        if (page.getId().pgcateg() == BTreePageId.LEAF) {
            BTreeLeafPage bpage = (BTreeLeafPage) page;
            bpage.checkRep(bt.keyField(), lowerBound, upperBound, checkOccupancy, depth);
            return new SubtreeSummary(bpage, depth);
        } else if (page.getId().pgcateg() == BTreePageId.INTERNAL) {

            BTreeInternalPage ipage = (BTreeInternalPage) page;
            ipage.checkRep(lowerBound, upperBound, checkOccupancy, depth);

            SubtreeSummary acc = null;
            BTreeEntry prev = null;
            Iterator<BTreeEntry> it = ipage.iterator();

            prev = it.next();
            { // init acc and prev.
                acc = checkSubTree(bt, tid, dirtypages, prev.getLeftChild(), lowerBound, prev.getKey(), ipage.getId(),
                        checkOccupancy, depth + 1);
                lowerBound = prev.getKey();
            }

            assert(acc != null);
            BTreeEntry curr = prev; // for one entry case.
            while (it.hasNext()) {
                curr = it.next();
                SubtreeSummary currentSubTreeResult =
                        checkSubTree(bt, tid, dirtypages, curr.getLeftChild(), lowerBound, curr.getKey(), ipage.getId(),
                                checkOccupancy, depth + 1);
                acc = SubtreeSummary.checkAndMerge(acc, currentSubTreeResult);

                // need to move stuff for next iter:
                lowerBound = curr.getKey();
            }

            SubtreeSummary lastRight = checkSubTree(bt, tid, dirtypages, curr.getRightChild(), lowerBound, upperBound,
                    ipage.getId(), checkOccupancy, depth + 1);
            acc = SubtreeSummary.checkAndMerge(acc, lastRight);

            return acc;
        } else {
            assert(false); // no other page types allowed inside the tree.
            return null;
        }
    }
}