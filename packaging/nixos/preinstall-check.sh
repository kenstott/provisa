# The NixOS check every Provisa installer makes: install.sh and the Linux AppImage's
# first-launch.sh both source this file, so a NixOS user is told one thing by either.
#
# NixOS has no loader or library at the paths a prebuilt binary expects, and its settings change
# only by a rebuild of the system from its configuration: an installer cannot make them. It checks
# for them, and where they are missing it names the steps and stops, having installed nothing.
# The settings are preinstall.nix, beside this file, which records its version on the host.
#
# The script that sources this provides: PROVISA_HOME, NON_INTERACTIVE, the ok/warn printers and
# the BOLD/CYAN/NC colours.

NIXOS_PREINSTALL_VERSION="1"
NIXOS_PREINSTALL_MARKER="/etc/provisa/nixos-preinstall"

is_nixos() { [ -e /etc/NIXOS ]; }

# nixos_preinstall <settings-file> <docker>
#   settings-file  where this installer carries preinstall.nix
#   docker         true when what is being installed runs under Docker; false for an install
#                  that needs none (the AppImage's native tier), which is not asked for it
# Returns when the host has what the install needs. Otherwise prints the steps and exits 1; the
# only thing it has written is the settings file, handed to the user in PROVISA_HOME.
nixos_preinstall() {
    local module_src="$1" docker_needed="$2"
    local settings=false docker_ready=true
    if [ -f "$NIXOS_PREINSTALL_MARKER" ] && [ "$(cat "$NIXOS_PREINSTALL_MARKER")" = "$NIXOS_PREINSTALL_VERSION" ]; then
        settings=true
    fi
    local in_group=true
    if [ "$docker_needed" = true ]; then
        in_group=false
        case " $(id -nG) " in *" docker "*) in_group=true ;; esac
        if [ "$in_group" = false ] || ! docker info &>/dev/null; then
            docker_ready=false
        fi
    fi
    if [ "$settings" = true ] && [ "$docker_ready" = true ]; then
        ok "NixOS: the settings Provisa needs are in place"
        return 0
    fi

    warn "This is NixOS, and it does not yet have the settings Provisa needs."
    warn "They are made in the system configuration, so the installer cannot make them for you."
    if [ "$NON_INTERACTIVE" = false ]; then
        # A closed stdin answers as Enter does.
        printf "${CYAN}[provisa]${NC} Press Enter to list the steps: "
        read -r _ || true
    fi

    local n=1
    printf "\n${BOLD}Before installing Provisa on NixOS:${NC}\n\n"
    if [ "$settings" = false ]; then
        local module="${PROVISA_HOME}/nixos-preinstall.nix"
        mkdir -p "${PROVISA_HOME}"
        cp "$module_src" "$module"
        printf "  %d. Copy the settings into the system configuration:\n" "$n"; n=$((n + 1))
        printf "       sudo cp %s /etc/nixos/provisa.nix\n" "$module"
        printf "  %d. In /etc/nixos/configuration.nix, add ./provisa.nix to imports:\n" "$n"; n=$((n + 1))
        printf "       imports = [ ./hardware-configuration.nix ./provisa.nix ];\n"
    fi
    if [ "$in_group" = false ]; then
        printf "  %d. In /etc/nixos/configuration.nix, give your user Docker:\n" "$n"; n=$((n + 1))
        printf "       users.users.%s.extraGroups = [ \"docker\" ];\n" "$(id -un)"
    fi
    printf "  %d. Apply the configuration:\n" "$n"; n=$((n + 1))
    printf "       sudo nixos-rebuild switch\n"
    printf "  %d. Log out and back in, then run this installer again.\n\n" "$n"
    exit 1
}
