export function Loading({ label = "Loading network data..." }) {
  return <div className="state-card">{label}</div>;
}

export function ErrorState({ message, title = "Unable to load data" }) {
  return (
    <div className="state-card error-state" role="alert">
      <strong>{title}</strong>
      <span>{message}</span>
    </div>
  );
}
