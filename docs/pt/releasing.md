# Lançamento (Releasing)

Releases são disparados ao enviar uma tag git. O nome da tag determina o canal.

## Convenções de Tag

| Formato de tag | Canal | Tipo de GitHub Release |
| ----------- | --------- | ------------------- |
| `v1.2.3-alpha.1` | alpha | Pre-release |
| `v1.2.3-beta.1` | beta | Pre-release |
| `v1.2.3-rc.1` | rc | Pre-release |
| `v1.2.3` | stable | Latest release |

## Criando um Release

```bash
# Alpha
git tag v1.2.3-alpha.1 && git push origin v1.2.3-alpha.1

# Beta
git tag v1.2.3-beta.1 && git push origin v1.2.3-beta.1

# Release candidate
git tag v1.2.3-rc.1 && git push origin v1.2.3-rc.1

# Stable
git tag v1.2.3 && git push origin v1.2.3
```

O workflow de CI (`build-dmg.yml`, chamado "Build Provisa Packages") dispara em qualquer tag `v*` e roda estes jobs, a maioria em paralelo:

1. **Resolver metadados de release** — detecta o canal a partir do sufixo da tag, deriva a versão PEP 440 e os nomes de asset
2. **Baixar / empacotar plugins Trino** — puxa conectores Calcite Trino e empacota um tarball
3. **Puxar imagens Docker core / obs / demo** — salva tarballs de imagem de serviço (arm64, mais amd64 core para o nível de container Windows)
4. **Build de DMGs Core / Obs / Demo macOS** — roda em `macos-14` (Apple Silicon), air-gapped
5. **Build do AppImage Linux** — core, air-gapped
6. **Build do instalador Core Windows** — nativo, Python embutido, sem Docker
7. **Build do instalador de nível Container Windows** — WSL2 + Trino, busca imagens sob demanda (sem VirtualBox/OVA)
8. **Build do driver JDBC** — JAR shaded do Maven
9. **Build e teste do cliente Python**, depois **Publicação no PyPI**
10. **Publicar GitHub Release** — envia todos os assets, define a flag de pre-release para alpha/beta/rc

## Assets do Release

Cada release publica os seguintes assets, todos anexados ao GitHub Release (o wheel também vai ao PyPI):

| Asset | Plataforma / Uso |
| ------- | ---------------- |
| `Provisa-<tag>-macOS.dmg` | macOS Core (Apple Silicon, air-gapped) |
| `Provisa-Runtime-<tag>-macOS.dmg` | Runtime Python nativo macOS (montar junto do Core) |
| `Provisa-Obs-<tag>-macOS.dmg` | Extensão de Observabilidade macOS |
| `Provisa-Demo-<tag>-macOS.dmg` | Extensão de Demonstração macOS (exige Obs) |
| `Provisa-<tag>-linux-x86_64.AppImage` | Core Linux x86_64 (air-gapped) |
| `Provisa-<tag>-windows-x64.exe` | Instalador nativo Windows x64 (Python embutido, sem Docker) |
| `Provisa-Container-<tag>-windows-x64.exe` | Upgrade de nível container Windows x64 (WSL2 + Trino) |
| `provisa-jdbc-<tag>.jar` | Driver JDBC — Tableau, PowerBI, DBeaver |
| `provisa_client-<pep440>-py3-none-any.whl` | Cliente Python (também PyPI) |
| `provisa-core-images-<tag>.tar.gz` | Tarballs de imagem dos Serviços Core (arm64, air-gapped) |
| `provisa-core-images-amd64-<tag>.zip` | Imagens dos Serviços Core (amd64, nível container Windows / air-gap) |
| `provisa-obs-images-<tag>.tar.gz` | Imagens da Stack de Observabilidade (opcional) |
| `provisa-demo-images-<tag>.tar.gz` | Imagens do Pacote de Dados de Demonstração (opcional) |
| `provisa-trino-plugins-<tag>.tar.gz` | Conectores do Motor de Coordenação (SharePoint, Splunk, File) |

A versão do cliente Python é automaticamente convertida para o formato PEP 440:
`v0.1.0-alpha.1` → `0.1.0a1`, `v0.1.0-beta.1` → `0.1.0b1`, `v0.1.0-rc.1` → `0.1.0rc1`.

## Configuração de Publicação PyPI (única vez)

1. Copie seu token de API de `~/.pypirc` (o valor `pypi-...` para `pypi.org`)
2. Adicione-o como um segredo de repositório nomeado `PYPI_API_TOKEN` em **Settings → Secrets → Actions**

O job `publish-pypi` então publicará automaticamente em cada tag.

## Segredos de Repositório Necessários

Configure estes em **Settings → Secrets → Actions**:

| Segredo | Necessário para | Descrição |
| -------- | ------------- | ------------- |
| `PYPI_API_TOKEN` | Publicação no PyPI | Token de API de `~/.pypirc` (começa com `pypi-`) |
| `APPLE_CERT_P12_BASE64` | Builds assinados | Arquivo de certificado `.p12` codificado em Base64 (veja abaixo) |
| `APPLE_CERT_P12_PASSWORD` | Builds assinados | Senha definida ao exportar o `.p12` do Keychain Access |
| `APPLE_DEVELOPER_ID` | Builds assinados | Nome completo do certificado: `Developer ID Application: Your Name (TEAMID)` |
| `APPLE_NOTARYTOOL_APPLE_ID` | Builds notarizados | E-mail do Apple ID |
| `APPLE_NOTARYTOOL_PASSWORD` | Builds notarizados | Senha específica do app de appleid.apple.com (não sua senha de login) |
| `APPLE_NOTARYTOOL_TEAM_ID` | Builds notarizados | ID de Team Apple de 10 caracteres |

Builds sem esses segredos são bem-sucedidos mas produzem um DMG não assinado/não notarizado (usuários verão um aviso do Gatekeeper).

## Exportando o Certificado .p12

1. Abra **Keychain Access** → login keychain → **My Certificates**
2. Encontre **Developer ID Application: Your Name (TEAMID)** — expanda para confirmar que a chave privada está aninhada abaixo
3. Selecione tanto o certificado quanto sua chave privada → clique com o botão direito → **Export 2 Items** → salve como `.p12` → defina uma senha forte
4. Codifique em Base64 e copie para a área de transferência:

   ```bash
   base64 -i YourCert.p12 | pbcopy
   ```

5. Cole como o valor de `APPLE_CERT_P12_BASE64`; defina `APPLE_CERT_P12_PASSWORD` para a senha do passo 3

## Encontrando o Nome do Seu Certificado

```bash
security find-identity -v -p codesigning | grep "Developer ID Application"
```

Copie a string completa entre aspas — esse é o valor para `APPLE_DEVELOPER_ID`.

## Excluindo uma Tag Ruim

```bash
git tag -d v1.2.3-alpha.1
git push origin :refs/tags/v1.2.3-alpha.1
```

Depois exclua o GitHub Release correspondente na UI antes de recriar a tag.
