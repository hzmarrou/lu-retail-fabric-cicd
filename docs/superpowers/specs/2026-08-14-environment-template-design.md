# Environment Template Design

## Goal

Provide a safe, discoverable template for the credentials and configuration used
by the Fabric tutorial without committing secrets.

## Files

- `.env.example` will contain every required variable with blank secret values
  and the tutorial's documented non-secret defaults.
- `.gitignore` will exclude `.env` while leaving `.env.example` tracked.

## Variables

The template will include the three service-principal credentials:

- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_ID`
- `FABRIC_CLIENT_SECRET`

It will also include the deployment pipeline, stage, and notebook names from the
tutorial. `FABRIC_TOKEN` will not be included because it is generated
temporarily during authentication.

## Security

Real credentials must be placed only in the untracked `.env` file or in GitHub
Actions secrets. The committed template will contain no credential values.

## Validation

Confirm that `.env` is ignored, `.env.example` remains trackable, and every
variable documented by the tutorial is represented exactly once.
