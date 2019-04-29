Example project showing a pattern of joins that can deadlock flink.

When run with a single TaskManager, 2 slots per TM, and parallelism 2, it completes
in under a minute on both a AWS c4 large and a Mid 2015 MacBookPro.

With 2 TaskManagers, 1 slot per TM, parallelism 2, it never completes.
