#!/bin/sh

set -e

help() {
    cat <<- EOF
Usage: TAG=tag $0

Updates version in go.mod files and pushes a new branch to GitHub.

VARIABLES:
  TAG        git tag, for example, v1.0.0
EOF
    exit 0
}

if [ -z "$TAG" ]
then
    printf "TAG is required\n\n"
    help
fi

TAG_REGEX="^v(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(\\-[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?(\\+[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?$"
if ! [[ "${TAG}" =~ ${TAG_REGEX} ]]; then
    printf "TAG is not valid: ${TAG}\n\n"
    exit 1
fi

TAG_FOUND=`git tag --list ${TAG}`
if [[ ${TAG_FOUND} = ${TAG} ]] ; then
    printf "tag ${TAG} already exists\n\n"
#    exit 1
fi

if ! git diff --quiet
then
    printf "working tree is not clean\n\n"
    git status
    exit 1
fi

git describe --tags

echo "what's the tag"

read tag

tag=$(echo "$tag" | sed 's/ //g')

export TAG=$tag

echo "Releasing $TAG"

sed -E -i 's/(const Version = )"[^"]*\"/\1"'"${TAG#v}"'\"/g' version.go
commit=$(git rev-parse HEAD)
sed -E -i 's/(const Commit = )"[^"]*\"/\1"'"${commit}"'\"/g' version.go
utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
sed -E -i 's/(const ReleaseTime = )"[^"]*\"/\1"'"${utc}"'\"/g' version.go

make lint
make test

git checkout -b release/${TAG}
git branch --track origin/release/${TAG}

git add .
git commit -m "Release:$TAG"
git tag $TAG