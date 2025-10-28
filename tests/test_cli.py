from __future__ import annotations

from amplitude_glue.cli import main


def test_cli_writes_report(tmp_path, capsys, samples_dir):
    output = tmp_path / "report.txt"

    main([
        "analyze",
        str(samples_dir / "streaming.json"),
        "--output",
        str(output),
    ])

    captured = capsys.readouterr()
    assert "Report saved" in captured.out
    assert output.exists()
    text = output.read_text(encoding="utf-8")
    assert "Warehouse Queries" in text
    assert len(text.strip()) > 0
