# UFDS watcher todo

2. handling UFDS not being there or going away - ensure reconnects.
  - preferably handle by wrapping in a vstream pipeline, unifying error events, etc.
  - what are correct ufds config settings here?
    - NB: CLS uses ldapclient, not UFDS, config slightly different (at least in password/credentials)?

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

