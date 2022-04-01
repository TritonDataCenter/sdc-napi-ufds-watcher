#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2022 Joyent, Inc.
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
SMF_MANIFESTS_IN = smf/manifests/napi-ufds-watcher.xml.in

NODE_PREBUILT_VERSION=v0.10.48
ifeq ($(shell uname -s),SunOS)
	NODE_PREBUILT_TAG=zone
	NODE_PREBUILT_IMAGE=18b094b0-eb01-11e5-80c1-175dac7ddf02
endif

include ./tools/mk/Makefile.defs
ifeq ($(shell uname -s),SunOS)
	include ./tools/mk/Makefile.node_prebuilt.defs
else
	NPM=npm
	NODE=node
	NPM_EXEC=$(shell which npm)
	NODE_EXEC=$(shell which node)
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
