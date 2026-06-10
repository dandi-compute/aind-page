# Transfer artifact (not part of aind-page)

`pozu-transcode-package.patch` is a relay artifact only. It contains the
`pozu-transcode` package + rich-click CLI commit, produced in a session that
could not push to `CodyCBakerPhD/pozu-transcode` directly.

Apply it there with:

    git checkout main
    git checkout -b claude/python-package-cli
    git am < pozu-transcode-package.patch

This directory should be deleted from aind-page after the transfer.
