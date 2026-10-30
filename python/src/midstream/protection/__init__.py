"""Data protection: masking sensitive fields before they reach the lake (handbook 22).

A materialization masks the values of fields that rules recognise as sensitive by their names -
emails, phone numbers, national identifiers, secrets and more - before the writer stages them, so
no component after the writer ever holds them: not the seal, not the data files, not the live tail,
not a query.

- `catalog`: the kinds, their rules and partial forms, the masks, and the default's text form.
- `rules`: recognising a kind from a field's name.
- `mask`: hashing, partial masks and withholding.
- `policy`: a materialization's policy and the table property recording what a table protects.
- `plan`: the per-record walk, and the committer's classification of fields.
- `secrets`: the protection key.

Import the submodules themselves: this package imports nothing, so the configuration can read the
catalog without loading the lakehouse.
"""
