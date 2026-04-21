import CanisterRBAC "mo:canister-rbac@0.1";
import Text "mo:base/Text";

/// Cumulative RBAC migration steps for CanisterDB.
///
/// Each function in this module represents one version's non-ZenDB upgrade work.
/// They are always called in order from the `with migration` fn in lib.mo.
///
/// RULE: never remove an old step — canisters can upgrade across multiple versions
/// in a single hop, so every step must be idempotent and cumulative.
/// All CanisterRBAC calls use `ignore` so they are no-ops when the state has
/// already been migrated (e.g. old name not found → #err → ignored).
module {

    public type Roles_v0_2_0 = {ADMIN: Text; EDITOR: Text; VIEWER: Text};
    public type Permissions_v0_2_0 = {DB_READ: Text; DB_WRITE: Text; DB_MANAGE: Text; ACCESS_CONTROL_READ: Text; ACCESS_CONTROL_MANAGE: Text};
    public type Resource_v0_2_0 = {DATABASE: Text; COLLECTION: Text};

        
    // v0.2.0 — renamed roles to clearer names
    //   "editor" → "writer"
    //   "viewer" → "reader"
    public func v0_2_0({
        canister_rbac : CanisterRBAC.Types.VersionedStableStore;

        // the following stable types are being removed (converted to transient)    
        // required to pass in these types so as we continue to upgrade the 
        // canister, we don't forget to update remove these which could end up 
        // causing older versions of the canister to break since these types are still used 
        // in the the canister being upgraded.
        Roles: Roles_v0_2_0;
        Permissions: Permissions_v0_2_0;
        Resource: Resource_v0_2_0;
    }) {
        ignore CanisterRBAC.renameRole(canister_rbac, "editor", "writer");
        ignore CanisterRBAC.renameRole(canister_rbac, "viewer", "reader");

    };

    public func applyAll({
        canister_rbac : CanisterRBAC.Types.VersionedStableStore;
        Roles : Roles_v0_2_0;
        Permissions : Permissions_v0_2_0;
        Resource : Resource_v0_2_0;
    }){
        v0_2_0({
            canister_rbac; 
            Roles; 
            Permissions; 
            Resource;
        });
    };

    public let CURRENT_API_VERSION = "0.2.1";

};
