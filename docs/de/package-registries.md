# Paket-Registries

Node- und Python-Quellen installieren ihren eigenen Code und ihre Abhängigkeiten. **Admin → Paket-Registries** legt fest, woher sie installieren: aus den npm-Registries, PyPI-Indizes und Git-Hosts der Organisation. Ein Organisationsadministrator trägt sie einmal ein, und jede Node- und Python-Installation der Organisation verwendet sie. Die öffentliche npm-Registry und das öffentliche PyPI brauchen keinen Eintrag.

Jedes Feld enthält, was eingegeben wurde: einen Wert oder eine Referenz wie `${secret:npm_token}` oder `${env:NPM_TOKEN}`, die bei jeder Installation im eigenen Vault der Organisation aufgelöst wird. Eine Referenz, die sich zu nichts auflöst, beendet die Installation und nennt die Registry und das Feld. Provisa schreibt nie in den Vault und erfindet keine Secret-Namen. Siehe [Secrets](secrets.md).

---

## npm-Registries {: #npm-registries }

| Feld | Enthält |
|---|---|
| Registry-URL | Die Adresse der Registry, etwa `https://artifactory.example.com/artifactory/api/npm/npm/` |
| Scope | Ein Scope wie `@acme`, dessen Pakete aus dieser Registry kommen. Leer für keinen |
| Token oder Benutzer:Passwort | Ein Zugriffstoken oder `user:password` |
| Jede Installation spiegeln | Jedes Paket ohne eigenen Scope wird über diese Registry installiert. Eine pro Organisation |
| Zertifizierungsstelle (PEM) | Siehe [Eine eigene Zertifizierungsstelle](#a-private-certificate-authority) |

Ein Token wird als `_authToken` in die `.npmrc` der Installation geschrieben und als Bearer-Token gesendet. `user:password`, etwa ein Artifactory-Benutzer und sein API-Schlüssel, wird als `_auth` (base64) geschrieben und als Basic gesendet. Beides geht nur an den eigenen Host der Registry.

## PyPI-Indizes {: #pypi-indexes }

| Feld | Enthält |
|---|---|
| Index-URL | Die Simple-Index-Adresse des Index, etwa `https://artifactory.example.com/artifactory/api/pypi/pypi/simple/` |
| Token oder Benutzer:Passwort | Ein Zugriffstoken, gesendet als Benutzer `__token__`, oder `user:password` |
| Standardindex für jede Installation | Jedes Python-Paket wird aus diesem Index statt aus dem öffentlichen PyPI installiert. Einer pro Organisation |
| Zertifizierungsstelle (PEM) | Siehe [Eine eigene Zertifizierungsstelle](#a-private-certificate-authority) |

Die Indizes und ihre Zugangsdaten erreichen uv nur in der eigenen Umgebung der Installation, nie in einer Datei.

## Git-Hosts {: #git-hosts }

Ein Git-Host-Eintrag deckt die Repositories ab, aus denen der Code einer Quelle oder ihre Git-Abhängigkeiten geholt werden.

| Feld | Enthält |
|---|---|
| Host | Der Host, mit Port und Pfadpräfix, wo der Server sie hat: `github.com`, `gitlab.example.com`, `bitbucket.example.com:7990/bitbucket` |
| Token oder Benutzer:Token | Ein Token oder `user:token` |
| Zertifizierungsstelle (PEM) | Siehe [Eine eigene Zertifizierungsstelle](#a-private-certificate-authority) |

Eine Repository-URL wird dem Eintrag zugeordnet, mit dessen Host, Port und Pfadpräfix sie beginnt. Passen mehrere, gewinnt der längste: Ein Eintrag für `bitbucket.example.com:7990/bitbucket/scm/data` deckt dieses Projekt ab, einer für `bitbucket.example.com:7990/bitbucket` den Rest.

Ein Token allein wird mit dem Benutzer `x-access-token` gesendet. Andere Anbieter erwarten einen Benutzer, also geben Sie `user:token` ein:

| Anbieter | Benutzer |
|---|---|
| GitHub | `x-access-token`, der Standard: das Token allein eingeben |
| GitLab | Ein beliebiger, nicht leerer Benutzername |
| Bitbucket Cloud | `x-token-auth` für ein Repository- oder Workspace-Zugriffstoken; Ihr Benutzername oder Ihre E-Mail-Adresse für ein API-Token |
| Bitbucket Data Center | Ihr Benutzername; `x-token-auth` für ein Projekt- oder Repository-Token |

## Eine eigene Zertifizierungsstelle {: #a-private-certificate-authority }

Eine Registry, ein Index oder ein Git-Server hinter Ihrer eigenen Zertifizierungsstelle, etwa ein Artifactory-Proxy in Ihrem Netz, braucht das Vertrauen in diese Stelle. Fügen Sie ihr PEM-Zertifikat in das Feld **Zertifizierungsstelle (PEM)** des Eintrags ein, oder eine Referenz auf ein Secret, das es enthält. Lassen Sie das Feld leer, wenn das Zertifikat des Servers öffentlich vertrauenswürdig ist. Ein Wert ohne PEM-Zertifikat wird beim Speichern abgelehnt, eine Referenz, die sich zu keinem auflöst, bei der Installation.

Der Stelle wird zusätzlich zu den öffentlich vertrauenswürdigen vertraut, nie an ihrer Stelle:

- Die eigenen Abrufe von Provisa über den Eintrag (Metadaten und Tarball eines Pakets, ein Wheel, ein Git-Repository) prüfen den Server gegen sie.
- Während einer Installation liest uv sie über `SSL_CERT_FILE`, npm und Bun über `cafile` in der eigenen `.npmrc` der Installation, Deno über `DENO_CERT` und Git über `GIT_SSL_CAINFO`.

Diese Einstellungen nennen eine Datei mit den öffentlich vertrauenswürdigen Stellen und denen jedes Eintrags. Die Datei wird in das temporäre Verzeichnis der Installation geschrieben und mit ihm entfernt, sodass nichts außerhalb der Installation sie liest.
