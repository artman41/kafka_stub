PROJECT = kafka_stub
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0
SP = 4

SHELL_DEPS :=
SHELL_DEPS += brod
SHELL_DEPS += lager

TEST_DEPS += meck

dep_brod = git https://github.com/kafka4beam/brod.git 3.8.1
dep_lager = git https://github.com/erlang-lager/lager.git e6b3178

dep_meck = git https://github.com/eproxus/meck.git v1.0.0

autopatch-brod::
	$(gen_verbose) rm -f $(DEPS_DIR)/brod/Makefile
	$(gen_verbose) $(call  dep_autopatch_for_rebar3,brod)

ERLC_OPTS :=
ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

SHELL_OPTS :=
SHELL_OPTS += -eval "application:ensure_all_started($(PROJECT))."
SHELL_OPTS += -config config/sys.config

include erlang.mk
