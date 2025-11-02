import ClientModule "Client";
import CanisterDBModule "CanisterDB";

module {
    public module Roles {
        public let MANAGER = "manager";
        public let USER = "user";
        public let GUEST = "guest";
    };

    public let { Client } = ClientModule;
    public type Client = ClientModule.Client;

    public let CanisterDB = CanisterDBModule.CanisterDB;
    public type CanisterDB = CanisterDBModule.CanisterDB;

};
