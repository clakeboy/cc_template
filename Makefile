NAME = cc_template
path = ./version
# amd64 arm 386
ARCH = amd64
#linux darwin windows
OS = linux
OUTPUTDIR = ./publish/
CC = clang
CXX = clang++

ifeq ($(OS),windows)
	OUTNAME = $(NAME).exe
	CC = /usr/local/opt/mingw-w64/bin/x86_64-w64-mingw32-gcc
	CXX = /usr/local/opt/mingw-w64/bin/x86_64-w64-mingw32-g++
else ifeq ($(OS),linux)
	OUTNAME = $(NAME)
	CC = /usr/local/gcc-4.8.1-for-linux64/bin/x86_64-pc-linux-gcc
    CXX = /usr/local/gcc-4.8.1-for-linux64/bin/x86_64-pc-linux-g++
else
	OUTNAME = $(NAME)_$(OS)
endif

APP_NAME = $(NAME)
APP_VERSION = $(shell bash ./makeversion.sh $(path))
BUILD_VERSION = $(shell date "+%s")
BUILD_TIME = $(shell date "+%FT%T%z")
GIT_REVISION = $(shell git rev-parse --short HEAD)
GIT_BRANCH = $(shell git name-rev --name-only HEAD)
GO_VERSION = $(shell go version)
#linux
all:
	CGO_ENABLED=0 GOARCH=$(ARCH) GOOS=$(OS) go build -x -v -ldflags "-s -w \
		-X 'main.AppName=$(APP_NAME)' \
		-X 'main.AppVersion=$(APP_VERSION)' \
		-X 'main.BuildVersion=$(BUILD_VERSION)' \
		-X 'main.BuildTime=$(BUILD_TIME)' \
		-X 'main.GitRevision=$(GIT_REVISION)' \
		-X 'main.GitBranch=$(GIT_BRANCH)' \
		-X 'main.GoVersion=$(GO_VERSION)' \
		" -o $(OUTPUTDIR)$(OUTNAME) main.go

	@if [ "darwin" != "darwin" ]; then\
		upx -9 $(OUTPUTDIR)$(OUTNAME); \
	fi

.PHONY : clean
clean:
	rm -f $(name)