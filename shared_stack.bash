#!/bin/bash

# Shared-stack maintenance.
#
# This tool builds and maintains a "shared stack" installation of the LSST
# Science Pipelines.
#
# End users looking to install the stack for personal use should prefer the
# procedure described in pipelines.lsst.io. Developers looking to build the
# latest versions of the LSST code should prefer the lsstsw build tool.
#
# Specifically, we:
#
# - Get the latest version of the lsstinstall tool into the STATE directory;
# - Get the list of tags from the EUPS distribution server;
# - For each tag which is not in the list of already-installed-and-deleted tags
#   in "tags.deleted" and which does not have a directory under $ROOT/tag:
#   - use lsstinstall to install the appropriate rubin-env conda environment if
#     needed
#   - add useful additional packages for developers if necessary
#   - use "eups distrib install" to install the Science Pipelines from binary
#     artifacts for the top-level PRODUCT
#   - create a loadLSST.sh script for users to load the environment from
#     scratch, a switch.sh script to activate the environment if conda is
#     already loaded, and an env_name file to simplify other tooling that may
#     need to map tags to rubin-env versions;
# - Maintain symlinks for the latest daily and weekly tags;
# - Maintain a common base loadLSST.sh script;
# - For each tag beyond the number to be kept for dailies (KEEP_d) and
#   weeklies (KEEP_w):
#   - add the tag to the "tags.deleted" list
#   - temporarily move the $ROOT/tag subdirectory aside
#   - untag (if shared) or remove (if exclusive) the eups packages with the tag
#   - remove the $ROOT/tag subdirectory
#
# A -n option may be used to do a "dry run" that only prints out the commands
# that would be executed.
#
# This code is intended to work properly if invoked multiple times in parallel,
# e.g. if it takes longer than the interval between cron jobs, although there
# is no resulting speedup.  Hence PID-labeled temporary directories and atomic
# moves are used frequently.

###############
# CONFIGURATION
###############

# Software root directory
ROOT=/scratch/rubinsw
# State directory
STATE=~/shared-stack
# Top-level product to install
PRODUCT=lsst_distrib
# Number of daily releases to keep
# shellcheck disable=SC2034
KEEP_d=24
# Number of weekly releases to keep
# shellcheck disable=SC2034
KEEP_w=26

#######
# Setup
#######

while getopts n opt; do
  case "$opt" in
    n) dryrun="echo"
       ;;
    *) echo "Usage: $0 [-n]"
       exit 1 ;;
  esac
done
[ $OPTIND -le $# ] && echo "Usage: $0 [-n]" && exit 1
# Ensure that there's no contamination from a pre-existing environment
unset LSST_CONDA_ENV_NAME PYTHONPATH

##############
# Installation
##############

set -e
mkdir -p "$STATE"
cd "$STATE"
# Label temporaries with our unique PID
tmp=tmp.$$
mkdir -p "$tmp" "$ROOT/tag"
[ -e tags.deleted ] || touch tags.deleted
umask 022

# Get list of tags
$dryrun curl -L https://ls.st/lsstinstall -o $tmp/lsstinstall
$dryrun mv $tmp/lsstinstall lsstinstall
curl -L https://eups.lsst.codes/stack/src/tags | while read -r line; do
  [[ "$line" =~ \>([dvw][0-9_]+(rc[0-9]+)?)\.list\< ]] && echo "${BASH_REMATCH[1]}"
done | sort > $tmp/tags

for tag in $(comm -1 -3 <(sort tags.deleted) $tmp/tags); do
  dir="$ROOT/tag/$tag"
  [ -d "$dir" ] && continue
  $dryrun mkdir -p "${dir}.$tmp" || continue
  (
     set -e
     $dryrun cd "${dir}.$tmp"
     $dryrun bash "$STATE/lsstinstall" -T "$tag" -p "$ROOT"/conda -S
     set +x
     $dryrun source loadLSST.sh
     set -x
     # Install the developer add-on packages if needed
     # When rubin-env-developer becomes available, replace with this:
     # conda list --json | grep rubin-env-developer > /dev/null \
     #   || mamba install rubin-env-developer
     if [ -n "$dryrun" ]; then
       echo "conda list --json | grep mypy > /dev/null || mamba install ..."
     else
       # shellcheck disable=SC2046
       conda list --json | grep mypy > /dev/null \
         || mamba install -y -c conda-forge --no-update-deps $(cat "$STATE/developer.txt")
     fi
     $dryrun eups distrib install -t "$tag" "$PRODUCT"

     # Rewrite load scripts and write useful files
     $dryrun cp loadLSST.sh tmp.loadLSST.sh
     if [ -n "$dryrun" ]; then
       echo "echo . $ROOT/loadLSST.sh ... > loadLSST.sh"
       echo "echo conda activate ... > switch.sh"
       echo "echo ... > env_name"
     else
       echo "LSST_CONDA_ENV_NAME=$LSST_CONDA_ENV_NAME . \"$ROOT/loadLSST.sh\"" > loadLSST.sh
       echo "conda activate $LSST_CONDA_ENV_NAME" > switch.sh
       echo "$LSST_CONDA_ENV_NAME" > env_name
     fi
  ) && [ ! -d "$dir" ] && $dryrun mv "${dir}.$tmp" "$dir"
  # Clean up if we failed or lost the race
  [ -d "${dir}.$tmp" ] && rm -rf "${dir}.$tmp"
done
rm $tmp/tags

# List the installed tags
(
  cd "$ROOT/tag"
  ls -dr d_* > "$STATE/$tmp/d_list" || true
  ls -dr w_* > "$STATE/$tmp/w_list" || true
)

# Make the latest links if needed
for type in d w; do
  [ -f "$tmp/${type}_list" ] || continue
  latest=$(head -n 1 "$tmp/${type}_list")
  link="$ROOT/${type}_latest"
  if [[ ! -L "$link" || "$(readlink "$link")" != "$ROOT/tag/$latest" ]]; then
    $dryrun rm -f "$link" && $dryrun ln -s "$ROOT/tag/$latest" "$link"
  fi
done
# Update loadLSST.sh if needed
tmp_load="$ROOT/w_latest/tmp.loadLSST.sh"
if [ -n "$dryrun" ]; then
  echo "[ -f \"$tmp_load\" ] && mv \"$tmp_load\" \"$ROOT/loadLSST.sh\""
elif [ -f "$tmp_load" ] && ! diff -q "$ROOT/loadLSST.sh" "$tmp_load" > /dev/null 2>&1; then
  [ -f "$ROOT/loadLSST.sh" ] && mv "$ROOT/loadLSST.sh" "$ROOT/loadLSST.sh.bak"
  mv "$tmp_load" "$ROOT/loadLSST.sh"
fi
$dryrun rm -f "$ROOT"/tag/*/tmp.loadLSST.sh
# Update current tag to latest weekly
# Doesn't handle all cases (an old weekly is installed as the latest in an
# even older environment), but should be good enough
if [ -f "$tmp/w_list" ]; then
  (
    latest=$(head -n 1 "$tmp/w_list")
    set -x
    $dryrun source "$ROOT/tag/$latest/loadLSST.sh"
    $dryrun eups tags --clone="$latest" current
  )
fi

#########
# Cleanup
#########

for type in d w; do
  [ -f "$tmp/${type}_list" ] || continue
  keep=KEEP_$type
  for tag_del in $(tail -n "+${!keep}" "$tmp/${type}_list"); do
      # Add to deleted list so this tag doesn't come back
      if [ -n "$dryrun" ]; then
        echo "echo $tag_del >> tags.deleted"
      else
        echo "$tag_del" >> tags.deleted || continue
      fi
      # Rename the directory so others can't use it
      del_dir="$ROOT/tag/del.$tag_del"
      $dryrun mv "$ROOT/tag/$tag_del" "$del_dir" || continue
      (
        set +x
        [ -n "$dryrun" ] && echo "source $del_dir/loadLSST.sh"
	# shellcheck disable=SC1091
        source "$del_dir/loadLSST.sh"
        set -x
        eups list -t "$tag_del" | while read -r pkg ver tag1 tagn; do
          [ -n "$dryrun" ] && echo "$pkg $ver $tag1 $tagn"
          # If this is the only tag (except current), remove the package
          # Otherwise, undeclare the tag
	  if [[ ( "$tag1" = "$tag_del" && ( "$tagn" = current || -z "$tagn" ) ) \
             || ( "$tag1" = current && "$tagn" = "$tag_del" ) ]]; then
            $dryrun eups remove --force -t "$tag_del" "$pkg"
          else
            $dryrun eups undeclare -t "$tag_del" "$pkg"
          fi
        done
      )
      $dryrun rm -rf "$del_dir"
   done
done
rm -f "$tmp"/[dw]_list
rmdir "$tmp"
