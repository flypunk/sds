%import utils
%steps_number = len(dep_steps)
%if steps_number > 1:
{{'There are %s steps in this deployment' % steps_number}}
%else:
{{'There is 1 step in this deployment'}}
%end
    %for step in dep_steps:
        %step_name = utils.gen_step_name(step)
        %if step_name:
        {{step_name}}
        %end        
    %end
