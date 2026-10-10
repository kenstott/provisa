# A NixOS host with none of the settings Provisa needs (preinstall.nix): stock NixOS. The NixOS
# workflow runs each installer here and expects the list of steps, not an install; with
# preinstall.nix beside it (the flake's `prepared`) it is the host those steps lead to.
{ pkgs, ... }:
{
  system.stateVersion = "26.05";

  users.users.provisa = {
    isNormalUser = true;
    extraGroups = [ "wheel" ];
  };
  security.sudo.wheelNeedsPassword = false;

  environment.systemPackages = [ pkgs.git ];
}
