release="$1"

if [[ -n $release ]]; then
  echo "Creating git tag with version $release"
  git commit --allow-empty -m "Release $release"
  git tag -a $release -m "Version $release"
  git push --tags 
else
  echo "Failed to create git tag. Pass a version to this script"
  exit 1
fi
