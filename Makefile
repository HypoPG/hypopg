EXTENSION = hypopg
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = hypo_setup \
	       hypo_index \
	       hypo_table

REGRESS_OPTS = --inputdir=test

PG_CONFIG ?= pg_config

MODULE_big = hypopg
OBJS = hypopg.o hypopg_import.o hypopg_analyze.o hypopg_index.o hypopg_table.o

all:

release-zip: all
	git archive --format zip --prefix=hypopg-${EXTVERSION}/ --output ./hypopg-${EXTVERSION}.zip HEAD
	unzip ./hypopg-$(EXTVERSION).zip
	rm ./hypopg-$(EXTVERSION).zip
	rm ./hypopg-$(EXTVERSION)/.gitignore
	rm ./hypopg-$(EXTVERSION)/docs/ -rf
	rm ./hypopg-$(EXTVERSION)/typedefs.list
	rm ./hypopg-$(EXTVERSION)/TODO.md
	sed -i -e "s/__VERSION__/$(EXTVERSION)/g"  ./hypopg-$(EXTVERSION)/META.json
	zip -r ./hypopg-$(EXTVERSION).zip ./hypopg-$(EXTVERSION)/
	rm ./hypopg-$(EXTVERSION) -rf


DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

DEBUILD_ROOT = /tmp/$(EXTENSION)

deb: release-zip
	mkdir -p $(DEBUILD_ROOT) && rm -rf $(DEBUILD_ROOT)/*
	unzip ./${EXTENSION}-$(EXTVERSION).zip -d $(DEBUILD_ROOT)
	cd $(DEBUILD_ROOT)/${EXTENSION}-$(EXTVERSION) && make -f debian/rules orig
	cd $(DEBUILD_ROOT)/${EXTENSION}-$(EXTVERSION) && debuild -us -uc -sa
