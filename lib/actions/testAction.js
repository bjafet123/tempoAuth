const {log, helpers} = require('utils-nxg-cg');
const {emits} = require("utils-nxg-cg/utils/constants");
const {producerErrorMessage} = require("msgbroker-nxg-cg");
const {isObjectValid} = require("utils-nxg-cg/utils/helpers");

module.exports.process = async function run(msg, cfg, snapshot) {
    try {
        log.info('Inside method run');
        log.debug('msg', msg);
        log.debug('cfg', cfg);
        log.debug('snapshot', snapshot);

        let properties = {auth: false};
        const {data} = msg;

        const valid = await helpers.validProperties(properties, data, cfg);

        if (valid) {
            if (!data.content) {
                data.content = 'c295IHVuIHRleHRvIGVuIGJhc2UgNjQ=';
            } else {
                data.content = helpers.convertBase64ToUtf8(data.content);
            }
        }

        if (!cfg.query) {
            cfg = {
                query: 'SELECT * FROM customers'
            }
        } else {
            cfg.query = 'UPDATE FROM customers WHERE customerId = 15'
        }

        let message = {
            metadata: {...msg.metadata, ...cfg},
            data: {
                ...data
            }
        }

        this.emit(emits.data, message);
        this.emit('end');
        console.log('Execution finished');
    } catch (e) {
        log.error(e);
        this.emit(emits.error, e);
        await producerErrorMessage(msg, e)
    }

};