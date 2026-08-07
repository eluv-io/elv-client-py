"""Ground truth pool management on top of the fabric client.

Mirrors the write flow of the video editor's ``GroundTruthStore``: assets are files in the
pool content object, and each entity's ``sample_files`` array links to them.
"""
