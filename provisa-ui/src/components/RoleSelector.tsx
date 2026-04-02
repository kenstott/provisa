import { useAuth } from "../context/AuthContext";

/** Role switcher — appears in the header. */
export function RoleSelector() {
  const { role, roles, setRole } = useAuth();

  if (roles.length === 0) return <span>No roles configured</span>;

  return (
    <select
      value={role?.id ?? ""}
      onChange={(e) => {
        const selected = roles.find((r) => r.id === e.target.value);
        setRole(selected ?? null);
      }}
    >
      {roles.map((r) => (
        <option key={r.id} value={r.id}>
          {r.id}
        </option>
      ))}
    </select>
  );
}
