# A NixOS host with none of the settings Provisa needs (preinstall.nix), for the installer's check:
# the NixOS workflow runs install.sh here and expects the list of steps, not an install.
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
