elibart
=======

NIF wrapper for libart (http://github.com/armon/libart).

This is the just a first quick & dirty version of the NIF. This is intended to be tested as memory storage for Lepasight Semantic Dataspace (http://leapsight.com/what_we_do.html).

To compile and test de library:

    # get deps and compile
    $ rebar compile
    # test the lib
    $ rebar eunit

TODO:
- Check there're no memory leaks.
- Sanitise Erlang API and NIF code.
- Add missing functions wrappers.
- Adapt insert and search functions to take arbitrary terms and not just binaries.
- Test, test, test.
