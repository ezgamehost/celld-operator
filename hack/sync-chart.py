"""Copy controller-gen schemas/RBAC into the customized Helm envelopes."""
from pathlib import Path

for source, target, key in [
    ("config/crd/bases/celld-operator.io_workerapps.yaml", "dist/chart/templates/crd/celld-operator.io_workerapps.yaml", "spec:"),
    ("config/rbac/role.yaml", "dist/chart/templates/rbac/role.yaml", "rules:"),
]:
    generated = Path(source).read_text()
    template = Path(target).read_text()
    prefix = template.split("\n" + key, 1)[0]
    body = generated.split("\n" + key, 1)[1]
    Path(target).write_text(prefix + "\n" + key + body.rstrip() + "\n{{- end }}\n")
