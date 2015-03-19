#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2015, Joyent, Inc.
#

#
# napi-ufds-watcher Makefile
#


#
# Tools
#


TAPE	:= ./node_modules/.bin/tape


#
# Files
#


RESTDOWN_FLAGS   = --brand-dir=deps/restdown-brand-remora
EXTRA_DOC_DEPS	= deps/restdown-brand-remora/.git
DOC_FILES	 = index.md
JS_FILES	:= $(shell ls *.js) $(shell find lib -name '*.js')
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -o indent=4,doxygen,unparenthesized-return=0
JSON_FILES  := package.json

include ./tools/mk/Makefile.defs
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.defs
else
	NPM_EXEC :=
	NPM = npm
endif
include ./tools/mk/Makefile.smf.defs


#
# Repo-specific targets
#


.PHONY: all
all: $(SMF_MANIFESTS) | $(TAPE) $(REPO_DEPS)
	$(NPM) install

$(TAPE): | $(NPM_EXEC)
	$(NPM) install

CLEAN_FILES += $(TAPE) ./node_modules/tape

.PHONY: test
test: $(TAPE)
	@(for F in test/unit/*.test.js; do \
		echo "# $$F" ;\
		$(NODE_EXEC) $(TAPE) $$F ;\
		[[ $$? == "0" ]] || exit 1; \
	done)


#
# Includes
#


include ./tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.targ
endif
include ./tools/mk/Makefile.smf.targ
include ./tools/mk/Makefile.targ
