import * as React from "react";
import moment = require("moment");

import {Comment} from "../model";

type CommentProps = Comment

export default class CommentComponent extends React.Component<CommentProps, {}> {
    render() {
        return (
            <div className="panel panel-danger comment">
                <div className="panel-heading comment-heading">
                    {`${this.props.author}  @ ${moment(this.props.dateTime).format('LLLL')}`}
                </div>
                <div className="panel-body">
                    {`${this.props.text}`}
                </div>
            </div>
        );
    }
}