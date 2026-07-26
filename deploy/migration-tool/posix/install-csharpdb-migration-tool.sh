#!/bin/sh

set -eu

usage() {
    cat <<'EOF'
Usage:
  sh install/posix/install-csharpdb-migration-tool.sh \
    --install-dir <directory> [--source-dir <archive-root>] [--force]

Copies an extracted, framework-dependent CSharpDB migration CLI release to a
caller-selected directory. The Microsoft .NET 10 runtime must already be
installed. The script does not require root, create a service, or change PATH.

The destination must be absent or empty. --force permits colliding files in a
nonempty destination to be overwritten; unrelated destination files are not
deleted.
EOF
}

fail() {
    printf '%s\n' "Error: $*" >&2
    exit 1
}

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
SOURCE_DIR=$SCRIPT_DIR/../..
INSTALL_DIR=
FORCE=0

while [ "$#" -gt 0 ]; do
    case "$1" in
        --install-dir)
            [ "$#" -ge 2 ] ||
                fail '--install-dir requires a value.'
            INSTALL_DIR=$2
            shift 2
            ;;
        --source-dir)
            [ "$#" -ge 2 ] ||
                fail '--source-dir requires a value.'
            SOURCE_DIR=$2
            shift 2
            ;;
        --force)
            FORCE=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "Unsupported option: $1"
            ;;
    esac
done

[ -n "$INSTALL_DIR" ] ||
    fail '--install-dir is required.'
[ -d "$SOURCE_DIR" ] ||
    fail "The extracted migration release directory does not exist: $SOURCE_DIR"
[ ! -L "$SOURCE_DIR" ] ||
    fail 'The extracted migration release directory cannot be a symbolic link.'
SOURCE_DIR=$(CDPATH= cd -- "$SOURCE_DIR" && pwd -P)

SOURCE_LINK=$(find "$SOURCE_DIR" -type l -print -quit)
[ -z "$SOURCE_LINK" ] ||
    fail 'The extracted migration release cannot contain symbolic links.'

for REQUIRED_FILE in \
    csharpdb \
    LICENSE \
    README.md \
    adapters/sqlserver/csharpdb-migration-sqlserver-worker \
    adapters/sqlserver/THIRD-PARTY-NOTICES.md \
    adapters/sqlserver/licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt \
    adapters/mysql/csharpdb-migration-mysql-worker \
    adapters/mysql/THIRD-PARTY-NOTICES.md
do
    [ -f "$SOURCE_DIR/$REQUIRED_FILE" ] ||
        fail "The extracted migration release is incomplete: $REQUIRED_FILE"
done

while [ "$INSTALL_DIR" != / ] &&
    [ "${INSTALL_DIR%/}" != "$INSTALL_DIR" ]
do
    INSTALL_DIR=${INSTALL_DIR%/}
done
[ "$INSTALL_DIR" != / ] ||
    fail 'The filesystem root cannot be used as the install directory.'
INSTALL_PARENT=$(dirname -- "$INSTALL_DIR")
INSTALL_NAME=$(basename -- "$INSTALL_DIR")
[ -n "$INSTALL_NAME" ] &&
    [ "$INSTALL_NAME" != . ] &&
    [ "$INSTALL_NAME" != .. ] ||
    fail 'The install directory must name one destination directory.'
[ -d "$INSTALL_PARENT" ] ||
    fail 'The install directory parent must already exist and be caller-controlled.'
[ ! -L "$INSTALL_PARENT" ] ||
    fail 'The install directory parent cannot be a symbolic link.'
INSTALL_PARENT=$(CDPATH= cd -- "$INSTALL_PARENT" && pwd -P)
INSTALL_PREFIX=${INSTALL_PARENT%/}/
INSTALL_DIR=$INSTALL_PREFIX$INSTALL_NAME
case "$INSTALL_DIR" in
    "$INSTALL_PREFIX"*)
        ;;
    *)
        fail 'The derived install directory must remain inside its existing parent.'
        ;;
esac

if [ -e "$INSTALL_DIR" ] || [ -L "$INSTALL_DIR" ]; then
    [ -d "$INSTALL_DIR" ] &&
        [ ! -L "$INSTALL_DIR" ] ||
        fail 'The install destination must be a real directory.'
    INSTALL_DIR=$(CDPATH= cd -- "$INSTALL_DIR" && pwd -P)
    case "$INSTALL_DIR/" in
        "$INSTALL_PREFIX"*)
            ;;
        *)
            fail 'The resolved install directory must remain inside its existing parent.'
            ;;
    esac

    DESTINATION_LINK=$(find "$INSTALL_DIR" -type l -print -quit)
    [ -z "$DESTINATION_LINK" ] ||
        fail 'The install destination cannot contain symbolic links, including with --force.'
fi

case "$INSTALL_DIR/" in
    "$SOURCE_DIR/"*)
        fail 'The install directory cannot be the extracted archive or a child of it.'
        ;;
esac
case "$SOURCE_DIR/" in
    "$INSTALL_DIR/"*)
        fail 'The extracted archive cannot be inside the install directory.'
        ;;
esac

if [ -e "$INSTALL_DIR" ] || [ -L "$INSTALL_DIR" ]; then
    if [ -n "$(command ls -A "$INSTALL_DIR")" ] &&
        [ "$FORCE" -ne 1 ]
    then
        fail 'The install destination is not empty. Pass --force to overwrite colliding files.'
    fi
fi

command -v dotnet >/dev/null 2>&1 ||
    fail 'The framework-dependent migration CLI requires the Microsoft .NET 10 runtime.'
dotnet --list-runtimes |
    grep -Eq '^Microsoft\.NETCore\.App 10\.' ||
    fail 'Install the Microsoft .NET 10 runtime before installing the migration CLI.'

mkdir -p -- "$INSTALL_DIR"
# The source and destination trees are link-free, so cp cannot redirect a
# derived child destination outside the checked install root.
command cp -Rp "$SOURCE_DIR"/. "$INSTALL_DIR"/
CLI_DESTINATION=$INSTALL_DIR/csharpdb
SQLSERVER_DESTINATION=$INSTALL_DIR/adapters/sqlserver/csharpdb-migration-sqlserver-worker
MYSQL_DESTINATION=$INSTALL_DIR/adapters/mysql/csharpdb-migration-mysql-worker
for DESTINATION_PATH in \
    "$CLI_DESTINATION" \
    "$SQLSERVER_DESTINATION" \
    "$MYSQL_DESTINATION"
do
    case "$DESTINATION_PATH" in
        "$INSTALL_DIR"/*)
            ;;
        *)
            fail 'A derived executable destination escapes the install directory.'
            ;;
    esac
done
chmod u+x \
    "$CLI_DESTINATION" \
    "$SQLSERVER_DESTINATION" \
    "$MYSQL_DESTINATION"

printf '%s\n' "Installed the CSharpDB migration CLI at $INSTALL_DIR"
printf '%s\n' "Run it directly as: $INSTALL_DIR/csharpdb"
printf 'PATH was not changed. For this shell, run: export PATH="%s:$PATH"\n' \
    "$INSTALL_DIR"
printf '%s\n' 'Add that export command to your shell profile only if you want it to persist.'
