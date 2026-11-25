# Provisa as a Nix package: the Linux release download, the AppImage, unpacked into the store and
# run in an FHS environment (appimageTools.wrapType2, a bubblewrap sandbox). Inside it the loader
# and the libraries sit at the paths a prebuilt binary expects, so the interpreter, wheels and
# embedded PostgreSQL that the first launch stages into ~/.provisa run on any Linux with Nix,
# NixOS without nix-ld among them.
#
# `provisa <command>` is the AppImage's own entry point: the first launch on a new version, then
# the provisa command (start, stop, status, ...).
#
# The AppImage is an argument whose default is the pinned release, so a user's flake gets that
# release. The NixOS workflow's `nix run` and module legs pass the AppImage their commit builds
# instead (`.override { releaseVersion = ...; releaseAppImage = ...; }`): they test what ships.
{
  lib,
  pkgs,
  appimageTools,
  fetchurl,
  cacert,
  jdk21,
  # The newest published release. Raise it, and its hash, with each release:
  #   nix store prefetch-file <url>
  releaseVersion ? "0.1.0-alpha.511",
  releaseAppImage ? fetchurl {
    url = "https://github.com/kenstott/provisa/releases/download/v${releaseVersion}/Provisa-v${releaseVersion}-linux-x86_64.AppImage";
    hash = "sha256-FVwwwm3Yx5KCl1f7cqWbiuIpCxjorrDZ6ahCExKfDZs=";
  },
}:
let
  # What a manylinux binary expects the distro to provide: the list preinstall.nix gives nix-ld.
  libraries = (import ./preinstall.nix { inherit pkgs; }).programs.nix-ld.libraries;
in
appimageTools.wrapType2 {
  pname = "provisa";
  version = releaseVersion;
  src = releaseAppImage;

  extraPkgs = _: libraries ++ [ jdk21 ];

  # A Python built for other distros finds no CA store of its own here, and an adapter server's
  # Python starts its JVM through JPype, which finds it by JAVA_HOME.
  profile = ''
    export SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt
    export JAVA_HOME=${jdk21.home}
  '';

  # `provisa start` starts the API, the UI server and the embedded PostgreSQL in the background
  # and returns. They keep the sandbox's mounts after it exits, and `provisa stop` finds them by
  # their process ids.
  unsharePid = false;
  dieWithParent = false;

  meta = {
    description = "Provisa, from its Linux release";
    homepage = "https://provisa.dev";
    license = lib.licenses.bsl11;
    sourceProvenance = [ lib.sourceTypes.binaryNativeCode ];
    platforms = [ "x86_64-linux" ];
    mainProgram = "provisa";
  };
}
