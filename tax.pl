:- use_module(library(csv)).

import_trades:-
    csv_read_file('./test-trades.csv', Data, [functor(transaction), arity(7)]),
    maplist(assert, Data).