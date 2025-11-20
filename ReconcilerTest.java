

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// --- Mock Domain Classes ---

class PendingTransaction {
    private final Long id;
    
    public PendingTransaction(Long id) {
        this.id = id;
    }
    
    public Long getId() { return id; }

    @Override
    public String toString() {
        return "PT(" + id + ")";
    }
}

class ProcessedTransaction {
    private final Optional<String> status;
    private final String id;
    
    public ProcessedTransaction(String id, Optional<String> status) {
        this.id = id;
        this.status = status;
    }
    
    public Optional<String> getStatus() { return status; } 
    public String getId() { return id; } 
}

// --- Reconciler Implementation 

class Reconciler {

    private <T> Stream<T> safeStream(Stream<T> stream) {
        return stream == null ? Stream.empty() : stream;
    }

    public Stream<PendingTransaction> reconcile(
            Stream<PendingTransaction> pendingTransactions,
            Stream<Stream<ProcessedTransaction>> processedTransactions) {

        Set<Long> processedIds;
        
        if (processedTransactions == null) {
            processedIds = Collections.emptySet();
        } else {
            processedIds = processedTransactions
                .flatMap(this::safeStream) 
                .filter(p -> p != null)
                .filter(p -> {
                    Optional<String> statusOpt = p.getStatus();
                    return statusOpt.isPresent() && "DONE".equalsIgnoreCase(statusOpt.get());
                })
                .map(ProcessedTransaction::getId)
                .filter(idString -> idString != null && !idString.isEmpty())
                // Assuming the "will certainly be a numeric value" guarantee holds.
                .map(idString -> {
                    try {
                        return Long.valueOf(idString);
                    } catch (NumberFormatException e) {
                        // Log or handle invalid numeric strings if robustness beyond the spec is required
                        return null; 
                    }
                })
                .filter(idLong -> idLong != null) // Filter out nulls from failed conversions
                .collect(Collectors.toSet());
        }

        return safeStream(pendingTransactions)
                .filter(p -> p != null)
                .filter(p -> processedIds.contains(p.getId()));
    }
}

// --- Test Class ---

public class ReconcilerTest {

    public static void main(String[] args) {
        Reconciler reconciler = new Reconciler();

        // TEST CASE 1: Basic Success (One transaction is DONE)
        testBasicSuccess(reconciler);

        // TEST CASE 2: Filtering by Status (ID match, but status is not DONE)
        testStatusMismatch(reconciler);

        // TEST CASE 3: Null/Empty Inputs (Input streams are null or empty)
        testNullAndEmptyStreams(reconciler);

        // TEST CASE 4: Nested Nulls and Empty Inner Streams
        testNestedNulls(reconciler);

        // TEST CASE 5: Case-Insensitive Status Check ("done")
        testCaseInsensitiveStatus(reconciler);
        
        // TEST CASE 6: Multiple IDs with different statuses and IDs
        testComplexMix(reconciler);

    }

    private static void assertTrue(boolean condition, String message) {
        if (condition) {
            System.out.println("[PASS] " + message);
        } else {
            System.err.println("[FAIL] " + message);
        }
    }

    // T1: Basic Success
    private static void testBasicSuccess(Reconciler reconciler) {
        // IDs: 101 (DONE), 102 (PENDING), 103 (NO MATCH)
        Stream<PendingTransaction> pending = Stream.of(
            new PendingTransaction(101L),
            new PendingTransaction(102L),
            new PendingTransaction(103L)
        );

        Stream<Stream<ProcessedTransaction>> processed = Stream.of(
            Stream.of(new ProcessedTransaction("101", Optional.of("DONE"))),
            Stream.of(new ProcessedTransaction("999", Optional.of("DONE"))) // Irrelevant
        );

        List<Long> resultIds = reconciler.reconcile(pending, processed)
            .map(PendingTransaction::getId)
            .collect(Collectors.toList());

        assertTrue(resultIds.size() == 1 && resultIds.contains(101L), 
                   "BasicSuccess: Should only return PT(101). Found: " + resultIds);
    }

    // T2: Filtering by Status
    private static void testStatusMismatch(Reconciler reconciler) {
        // IDs: 201 (DONE), 202 (PENDING status), 203 (NULL status)
        Stream<PendingTransaction> pending = Stream.of(
            new PendingTransaction(201L),
            new PendingTransaction(202L),
            new PendingTransaction(203L)
        );

        Stream<Stream<ProcessedTransaction>> processed = Stream.of(
            Stream.of(new ProcessedTransaction("201", Optional.of("DONE"))),
            Stream.of(new ProcessedTransaction("202", Optional.of("PENDING"))),
            Stream.of(new ProcessedTransaction("203", Optional.empty()))
        );

        List<Long> resultIds = reconciler.reconcile(pending, processed)
            .map(PendingTransaction::getId)
            .collect(Collectors.toList());

        assertTrue(resultIds.size() == 1 && resultIds.contains(201L), 
                   "StatusMismatch: Should return PT(201). Found: " + resultIds);
    }
    
    // T3: Null/Empty Inputs
    private static void testNullAndEmptyStreams(Reconciler reconciler) {
        // Case A: Both inputs null
        List<Long> resultA = reconciler.reconcile(null, null)
            .map(PendingTransaction::getId)
            .collect(Collectors.toList());
        assertTrue(resultA.isEmpty(), "NullInputs A: Both null, should return empty stream.");

        // Case B: Pending null, Processed empty
        List<Long> resultB = reconciler.reconcile(null, Stream.empty())
            .map(PendingTransaction::getId)
            .collect(Collectors.toList());
        assertTrue(resultB.isEmpty(), "NullInputs B: Pending null, Processed empty, should return empty stream.");

        // Case C: Processed null, Pending non-empty
        Stream<PendingTransaction> pendingC = Stream.of(new PendingTransaction(301L));
        List<Long> resultC = reconciler.reconcile(pendingC, null)
            .map(PendingTransaction::getId)
            .collect(Collectors.toList());
        assertTrue(resultC.isEmpty(), "NullInputs C: Processed null, should return empty stream.");
    }

    // T4: Nested Nulls
    private static void testNestedNulls(Reconciler reconciler) {
        // ID 401: Valid (DONE)
        // ID 402: Invalid ID string ("INVALID")
        // ID 403: Processed object is null
        // ID 404: Valid (DONE)
        Stream<PendingTransaction> pending = Stream.of(
            new PendingTransaction(401L),
            new PendingTransaction(402L),
            new PendingTransaction(403L),
            new PendingTransaction(404L)
        );

        Stream<Stream<ProcessedTransaction>> processed = Stream.of(
            null, // Null inner stream
            Stream.of(new ProcessedTransaction("401", Optional.of("DONE"))),
            Stream.of(new ProcessedTransaction("INVALID", Optional.of("DONE"))), // Invalid ID string
            Stream.of(null, new ProcessedTransaction("404", Optional.of("DONE"))) // Null element
        );

        List<Long> resultIds = reconciler.reconcile(pending, processed)
            .map(PendingTransaction::getId)
            .collect(Collectors.toList());

        assertTrue(resultIds.size() == 2 && resultIds.contains(401L) && resultIds.contains(404L), 
                   "NestedNulls: Should return PT(401, 404). Found: " + resultIds);
    }
    
    // T5: Case-Insensitive Status
    private static void testCaseInsensitiveStatus(Reconciler reconciler) {
        // IDs: 501 (DONE), 502 (done), 503 (dOnE), 504 (not done)
        Stream<PendingTransaction> pending = Stream.of(
            new PendingTransaction(501L),
            new PendingTransaction(502L),
            new PendingTransaction(503L),
            new PendingTransaction(504L)
        );

        Stream<Stream<ProcessedTransaction>> processed = Stream.of(
            Stream.of(new ProcessedTransaction("501", Optional.of("DONE"))),
            Stream.of(new ProcessedTransaction("502", Optional.of("done"))),
            Stream.of(new ProcessedTransaction("503", Optional.of("dOnE"))),
            Stream.of(new ProcessedTransaction("504", Optional.of("NOT DONE")))
        );

        Set<Long> expected = Set.of(501L, 502L, 503L);
        List<Long> resultIds = reconciler.reconcile(pending, processed)
            .map(PendingTransaction::getId)
            .collect(Collectors.toList());

        assertTrue(resultIds.size() == 3 && resultIds.containsAll(expected), 
                   "CaseInsensitiveStatus: Should return PT(501, 502, 503). Found: " + resultIds);
    }
    
    // T6: Complex Mix (Testing duplicate IDs, irrelevant IDs, and multiple inner streams)
    private static void testComplexMix(Reconciler reconciler) {
        // Pending: 601 (DONE), 602 (PENDING), 603 (NO MATCH), 604 (ID string null)
        Stream<PendingTransaction> pending = Stream.of(
            new PendingTransaction(601L), 
            new PendingTransaction(602L),
            new PendingTransaction(603L),
            new PendingTransaction(604L)
        );

        Stream<Stream<ProcessedTransaction>> processed = Stream.of(
            // Inner Stream 1: Valid DONE
            Stream.of(
                new ProcessedTransaction("601", Optional.of("DONE")),
                new ProcessedTransaction("999", Optional.of("DONE")) // Irrelevant ID
            ),
            // Inner Stream 2: PENDING status
            Stream.of(
                new ProcessedTransaction("602", Optional.of("PENDING")) // Should be filtered out
            ),
            // Inner Stream 3: Null String ID
            Stream.of(
                new ProcessedTransaction(null, Optional.of("DONE")) // Null ID String
            )
        );

        List<Long> resultIds = reconciler.reconcile(pending, processed)
            .map(PendingTransaction::getId)
            .collect(Collectors.toList());

        assertTrue(resultIds.size() == 1 && resultIds.contains(601L), 
                   "ComplexMix: Should only return PT(601). Found: " + resultIds);
    }

}


