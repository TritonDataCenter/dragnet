#
# Copyright (c) 2014, Joyent, Inc. All rights reserved.
#
# Makefile: top-level Makefile
#
# This Makefile contains only repo-specific logic and uses included makefiles
# to supply common targets (javascriptlint, jsstyle, restdown, etc.), which are
# used by other repos as well.
#

#
# Tools must be installed on the path
# JSL is JavaScriptLint, from https://github.com/davepacheco/javascriptlint
# JSSTYLE is jsstyle, from https://github.com/davepacheco/jsstyle
# MD2MAN is md2man-roff, from https://github.com/sunaku/md2man
#
CATEST		 = ./tools/catest
JSL		 = jsl
JSSTYLE		 = jsstyle
MD2MAN		 = md2man-roff

#
# Files
#
JSON_FILES	 = package.json
JS_FILES	:= $(shell find lib schema -name '*.js')
JS_FILES	+= bin/dn \
		   tools/pathenum \
		   tools/mktestdata \
		   tools/validate-schema
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSL_CONF_NODE	 = tools/jsl.node.conf

all:
	npm install

test:
	$(CATEST) -a

include ./Makefile.targ
include ./Makefile.man.targ
