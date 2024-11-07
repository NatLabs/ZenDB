// actor.ts
import { canisterId, idlFactory, createActor } from "./declarations/backend"
import { createReactor } from "@ic-reactor/react"

export const backend = createActor(canisterId)

type Actor = typeof backend

export const { useActorStore, useAuth, useQueryCall } = createReactor<Actor>({
    canisterId,
    idlFactory,
    host: "https://localhost:4943",
})
