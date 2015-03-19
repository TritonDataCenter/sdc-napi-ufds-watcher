# UFDS watcher todo

1. correct overlay network/vlan creation in default-fabric-setup.js

2. handling UFDS not being there or going away - ensure reconnects.
  - preferably handle by wrapping in a vstream pipeline, unifying error events, etc.
  - what are correct ufds config settings here?
    - NB: CLS uses ldapclient, not UFDS, config slightly different (at least in password/credentials)?

3. changelog tracking class, which must:
  - add changelog number following the filter step
  - remove changelog number when:
    - succesfully updating user's dclocalconfig
    - user not found to begin with
  - update permanent storage periodically
    - must be locking; i.e., don't want async updates racing
  - warn on lack of progress?
Options here:
  1. pair of streams that add/remove from a shared list.
    - depends on having appropriate state to make that call in the object at all times (don't think we have that?)
  2. make tasks aware of the counter & use it. Less general, more branches to update.

4. enable/disable service based on sdc-application metadata
  - disable itself in the smf start method, based on what's in our own config?
  - does this race with config-agent? (if so, fix smf service deps?)

5. testing
  - current test methodology doc'd
  - test approach outlined
  - start writing tests

7. Cleanup/make rigorous logging.
  - log status of queues @ debug/trace level
  - kang endpoint for those?

