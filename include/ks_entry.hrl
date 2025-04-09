-ifndef(KS_ENTRY_HRL).
-define(KS_ENTRY_HRL, 1).

-record(ks_entry, {
    topic_name :: binary(),
    current_offset :: non_neg_integer(),
    tab :: ets:tab()
}).

-endif.